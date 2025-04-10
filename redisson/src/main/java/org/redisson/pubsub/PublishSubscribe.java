/**
 * Copyright (c) 2013-2024 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.pubsub;

import org.redisson.PubSubEntry;
import org.redisson.client.BaseRedisPubSubListener;
import org.redisson.client.ChannelName;
import org.redisson.client.RedisPubSubListener;
import org.redisson.client.codec.LongCodec;
import org.redisson.misc.AsyncSemaphore;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 
 * @author Nikita Koksharov
 *
 */
abstract class PublishSubscribe<E extends PubSubEntry<E>> {

    private final ConcurrentMap<String, E> entries = new ConcurrentHashMap<>();
    private final PublishSubscribeService service;

    PublishSubscribe(PublishSubscribeService service) {
        super();
        this.service = service;
    }

    public void unsubscribe(E entry, String entryName, String channelName) {
        ChannelName cn = new ChannelName(channelName);
        AsyncSemaphore semaphore = service.getSemaphore(cn);
        semaphore.acquire().thenAccept(c -> {
            ///  마지막 구독자일때
            if (entry.release() == 0) {
                entries.remove(entryName);
                service.unsubscribeLocked(cn)
                        .whenComplete((r, e) -> {
                            semaphore.release();
                        });
            ///  아직 구독자가 남아있을 때
            } else {
                semaphore.release();
            }
        });
    }

    public void timeout(CompletableFuture<?> promise) {
        service.timeout(promise);
    }

    public void timeout(CompletableFuture<?> promise, long timeout) {
        service.timeout(promise, timeout);
    }

    // 실제 구독할때 쓰는 메서드
    public CompletableFuture<E> subscribe(String entryName, String channelName) {
        AsyncSemaphore semaphore = service.getSemaphore(new ChannelName(channelName));
        CompletableFuture<E> newPromise = new CompletableFuture<>();

        semaphore.acquire().thenAccept(c -> {
            if (newPromise.isDone()) {
                semaphore.release();
                return;
            }

            // 같은 entryName 에 대해 동일한 entry 객체를 쓰도록 함.
                // entryName = id + ":" + name; -> 같은 레디슨클라이언트에서 같은 락 쓰는 스레드들은 같은 엔트리를 사용함.
            // Lock 에서는 <String, RedissonLockEntry> 자료형임.

            /// 기존 엔트리가 이미 존재하는 경우
            E entry = entries.get(entryName);
            if (entry != null) {
                entry.acquire(); // 카운터 1 증가; 몇 명이 락 획득을 기다리고 있는 중인지 나타냄
                semaphore.release(); // 세마포어의 허용량을 1 늘리고, 다른 스레드가 작업을 시작할 수 있도록 함. 이때 다른 스레드가 semaphore.acquire().thenAccept(); 이 블록 안으로 들어갈 수 있게 되는 것임.
                // whenComplete는 CompletableFuture가 완료될 때 실행할 작업을 정의한다.
                entry.getPromise().whenComplete((r, e) -> {
                    // 여기서의 r은 RedissonLockEntity의 객체로, 구독 상태를 관리한다. 여기서는 기존 구독에 참여하게 되는 것.
                    if (e != null) {
                        newPromise.completeExceptionally(e);
                        return;
                    }
                    newPromise.complete(r);
                });
                return;
            }

            ///  기존 엔트리가 존재하지 않는 경우
            E value = createEntry(newPromise);
            value.acquire();
            /// 엔트리 등록: entries 에 value 넣는데, 경쟁 조건에서 다른 스레드가 이미 등록한 엔트리가 있으면 그걸 사용함!!!
            E oldValue = entries.putIfAbsent(entryName, value);
            // 이하 로직은 기존 엔트리가 이미 존재하는 경우와 동일
            if (oldValue != null) {
                oldValue.acquire();
                semaphore.release();
                oldValue.getPromise().whenComplete((r, e) -> {
                    if (e != null) {
                        newPromise.completeExceptionally(e);
                        return;
                    }
                    newPromise.complete(r);
                });
                return;
            }

            // Redis 채널에 대해 메시지를 수신할 리스너를 생성하고 등록
            // 이 리스너는 락 해제 메시지가 올 때 동작하게 됨
            RedisPubSubListener<Object> listener = createListener(channelName, value);
            CompletableFuture<PubSubConnectionEntry> s = service.subscribeNoTimeout(LongCodec.INSTANCE, channelName, semaphore, listener);
            newPromise.whenComplete((r, e) -> {
                if (e != null) {
                    s.completeExceptionally(e);
                }
            });
            s.whenComplete((r, e) -> {
                if (e != null) {
                    entries.remove(entryName);
                    value.getPromise().completeExceptionally(e);
                    return;
                }
                if (!value.getPromise().complete(value)) {
                    if (value.getPromise().isCompletedExceptionally()) {
                        entries.remove(entryName);
                    }
                }
            });

        });

        // 이게 subscribe 함수의 실제 반환값임!!!
            // 참고: thenAccept 블록 안에서 나오는 return 은 이 블록을 종료하는 역할을 함.
        return newPromise;
    }

    protected abstract E createEntry(CompletableFuture<E> newPromise);

    protected abstract void onMessage(E value, Long message);

    private RedisPubSubListener<Object> createListener(String channelName, E value) {
        RedisPubSubListener<Object> listener = new BaseRedisPubSubListener() {

            @Override
            public void onMessage(CharSequence channel, Object message) {
                if (!channelName.equals(channel.toString())) {
                    return;
                }

                PublishSubscribe.this.onMessage(value, (Long) message);
            }
        };
        return listener;
    }

}
