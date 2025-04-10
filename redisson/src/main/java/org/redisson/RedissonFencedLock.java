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
package org.redisson;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import org.redisson.api.RFencedLock;
import org.redisson.api.RFuture;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.client.protocol.RedisStrictCommand;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.misc.CompletableFutureWrapper;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Redis based implementation of Fenced Lock with reentrancy support.
 * <p>
 * Each lock acquisition increases fencing token. It should be
 * checked if it's greater or equal with the previous one by
 * the service guarded by this lock and reject operation if condition is false.
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonFencedLock extends RedissonLock implements RFencedLock {

    private final String tokenName;

    public RedissonFencedLock(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        tokenName = prefixName("redisson_lock_token", getRawName());
    }

    @Override
    public Long getToken() {
        return get(getTokenAsync());
    }

    @Override
    public RFuture<Long> getTokenAsync() {
        return commandExecutor.writeAsync(tokenName, StringCodec.INSTANCE, RedisCommands.GET_LONG, tokenName);
    }

    @Override
    public Long lockAndGetToken() {
        return get(lockAndGetTokenAsync());
    }

    public RFuture<Long> lockAndGetTokenAsync() {
        return tryLockAndGetTokenAsync(-1, -1, null);
    }

    @Override
    public Long lockAndGetToken(long leaseTime, TimeUnit unit) {
        return get(lockAndGetTokenAsync());
    }

    public RFuture<Long> lockAndGetTokenAsync(long leaseTime, TimeUnit unit) {
        return tryLockAndGetTokenAsync(-1, leaseTime, unit);
    }

    private <T> RFuture<List<Long>> tryAcquireAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId) {
        RFuture<List<Long>> ttlRemainingFuture;
        if (leaseTime > 0) {
            ttlRemainingFuture = tryLockInnerAsync(leaseTime, unit, threadId);
        } else {
            ttlRemainingFuture = tryLockInnerAsync(internalLockLeaseTime, TimeUnit.MILLISECONDS, threadId);
        }
        CompletionStage<List<Long>> f = ttlRemainingFuture.thenApply(res -> {
            Long ttl = res.get(0);
            // lock acquired
            if (ttl == -1) {
                if (leaseTime > 0) {
                    internalLockLeaseTime = unit.toMillis(leaseTime);
                } else {
                    scheduleExpirationRenewal(threadId);
                }
            }
            return res;
        });
        return new CompletableFutureWrapper<>(f);
    }

    RFuture<List<Long>> tryLockInnerAsync(long leaseTime, TimeUnit unit, long threadId) {
        return commandExecutor.syncedEvalNoRetry(getRawName(), LongCodec.INSTANCE, RedisCommands.EVAL_LONG_LIST,
                "if (redis.call('exists', KEYS[1]) == 0 " +
                        "or (redis.call('hexists', KEYS[1], ARGV[2]) == 1)) then " +
                            "local token = redis.call('incr', KEYS[2]);" +
                            "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                            "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                            "return {-1, token}; " +
                        "end; " +
                        "return {redis.call('pttl', KEYS[1]), -1};",
                Arrays.asList(getRawName(), tokenName),
                unit.toMillis(leaseTime), getLockName(threadId));
    }

    @Override
    public Long tryLockAndGetToken() {
        return get(tryLockAndGetTokenAsync());
    }

    public RFuture<Long> tryLockAndGetTokenAsync() {
        return tryLockAndGetTokenAsync(-1, null);
    }

    @Override
    public Long tryLockAndGetToken(long waitTime, long leaseTime, TimeUnit unit) {
        return get(tryLockAndGetTokenAsync(waitTime, leaseTime, unit));
    }

    public RFuture<Long> tryLockAndGetTokenAsync(long waitTime, long leaseTime, TimeUnit unit) {
        return tryLockAndGetTokenAsync(waitTime, leaseTime, unit, Thread.currentThread().getId());
    }

    public Long tryLockAndGetToken(long waitTime, TimeUnit unit) {
        return get(tryLockAndGetTokenAsync(waitTime, unit));
    }

    public RFuture<Long> tryLockAndGetTokenAsync(long waitTime, TimeUnit unit) {
        return tryLockAndGetTokenAsync(waitTime, -1, unit);
    }

    public RFuture<Long> tryLockAndGetTokenAsync(long waitTime, long leaseTime, TimeUnit unit,
                                                 long currentThreadId) {
        CompletableFuture<Long> result = new CompletableFuture<>();

        AtomicLong time;
        if (waitTime < 0) {
            time = new AtomicLong(Long.MAX_VALUE);
        } else {
            time = new AtomicLong(unit.toMillis(waitTime));
        }
        long currentTime = System.currentTimeMillis();
        RFuture<List<Long>> ttlFuture = tryAcquireAsync(waitTime, leaseTime, unit, currentThreadId);
        ttlFuture.whenComplete((res, e) -> {
            if (e != null) {
                result.completeExceptionally(e);
                return;
            }

            Long ttl = res.get(0); // 락 성공시 {-1, 펜싱토큰} 반환, 실패시 {pttl, -1} 반환
            // 즉, ttl = -1 또는 남은 ttl 값
            // lock acquired
            if (ttl == -1) {
                if (!result.complete(res.get(1))) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            // 다른 스레드가 락을 소유하고 있는 경우(ttl != -1)
            long el = System.currentTimeMillis() - currentTime;
            time.addAndGet(-el);

            // 만약 대기 시간 초과시 -> null 반환(락 획득 실패)
            if (time.get() <= 0) {
                result.complete(null);
                return;
            }

            long current = System.currentTimeMillis();
            AtomicReference<Timeout> futureRef = new AtomicReference<>();
            // 락 시도 실패자들 구독 시작!
            CompletableFuture<RedissonLockEntry> subscribeFuture = subscribe(currentThreadId);
            // 대기 시간 내에 구독 완료 안되면 취소함(타임아웃 설정).
            pubSub.timeout(subscribeFuture, time.get());

            // 구독 완료 후 재시도
            subscribeFuture.whenComplete((r, ex) -> {
                if (ex != null) {
                    result.completeExceptionally(ex);
                    return;
                }

                if (futureRef.get() != null) {
                    futureRef.get().cancel();
                }

                long elapsed = System.currentTimeMillis() - current;
                time.addAndGet(-elapsed);

                // 재시도 로직
                tryLockAsync(time, waitTime, leaseTime, unit, r, result, currentThreadId);
            });
            // 구독 완료 안될경우, 타임아웃 작업 실행 예약함. -> 구독 실패시 null 반환을 보장함.
            if (!subscribeFuture.isDone()) {
                Timeout scheduledFuture = commandExecutor.getServiceManager().newTimeout(new TimerTask() {
                    @Override
                    public void run(Timeout timeout) throws Exception {
                        if (!subscribeFuture.isDone()) {
                            subscribeFuture.cancel(false);
                            result.complete(null);
                        }
                    }
                }, time.get(), TimeUnit.MILLISECONDS);
                futureRef.set(scheduledFuture);
            }
        });


        return new CompletableFutureWrapper<>(result);
    }

    private void tryLockAsync(AtomicLong time, long waitTime, long leaseTime, TimeUnit unit,
                              RedissonLockEntry entry, CompletableFuture<Long> result, long currentThreadId) {
        // 결과가 이미 완료된 경우 -> 구독 해제 후 종료
        if (result.isDone()) {
            unsubscribe(entry, currentThreadId);
            return;
        }

        // 대기 시간 초과된 경우 -> 구독 해제, null 반환 후 종료
        if (time.get() <= 0) {
            unsubscribe(entry, currentThreadId);
            result.complete(null);
            return;
        }

        // 현재 시간 기록
        long curr = System.currentTimeMillis();
        // 락 재시도 (다시 tryAcquireAsync 호출)
        RFuture<List<Long>> ttlFuture = tryAcquireAsync(waitTime, leaseTime, unit, currentThreadId);
        // 재시도한 결과 처리
        ttlFuture.whenComplete((res, e) -> {
            // 예외 발생시 처리: 구독 해제, result에 예외 전달하고 종료
            if (e != null) {
                unsubscribe(entry, currentThreadId);
                result.completeExceptionally(e);
                return;
            }

            // 락 획득 성공 여부 확인
            Long ttl = res.get(0);
            // lock acquired
            // 락 획득 성공 -> ttl = -1 됨.
            if (ttl == -1) {
                // 구독 해제
                unsubscribe(entry, currentThreadId);
                // result에 토큰 값으로 값을 채우는데, 실패하면 강제로 락 해지 - res[1]에 토큰 있어야함. complete()는 compareAndSet()임
                if (!result.complete(res.get(1))) {
                    unlockAsync(currentThreadId);
                }
                return;
            }

            // 락 획득 실패시
            // 경과 시간 계산 및 반영
            long el = System.currentTimeMillis() - curr;
            time.addAndGet(-el);
            // 대기 시간 초과 여부 확인
            if (time.get() <= 0) {
                unsubscribe(entry, currentThreadId);
                result.complete(null);
                return;
            }

            // waiting for message
            // 락 해제 알림 대기
            // 현재 시간 다시 기록
            long current = System.currentTimeMillis();
            // 지금 바로 락 재시도 가능한지 확인하는 로직 - 이게 true 라면, 락 해제 알림 와서 -> latch 허용량이 증가한 상태였다는 것.
            if (entry.getLatch().tryAcquire()) {
                // 재시도 가능하면 바로 다시 tryLockAsync 재귀 호출!
                tryLockAsync(time, waitTime, leaseTime, unit, entry, result, currentThreadId);
            }
            // 재시도 불가능하면 리스너 등록 후 알림 대기
            else {
                AtomicBoolean executed = new AtomicBoolean(); // 리스너가 실행됐는지 여부를 추적하는 플래그
                AtomicReference<Timeout> futureRef = new AtomicReference<>(); // 타임아웃 작업을 추적하는 변수
                // 리스너 정의
                Runnable listener = () -> {
                    executed.set(true); // 리스너가 실행 완료됨 표시
                    if (futureRef.get() != null) {
                        // 리스너가 먼저 실행됐으니 타임아웃으로 예약된 작업을 취소함
                        futureRef.get().cancel();
                    }

                    long elapsed = System.currentTimeMillis() - current;
                    time.addAndGet(-elapsed); // 경과 시간 반영

                    tryLockAsync(time, waitTime, leaseTime, unit, entry, result, currentThreadId);
                };
                // 리스너 등록 -> latch의 허용량이 증가하면(즉, entry.getLatch().release() 호출 시) 등록된 listener를 실행되도록 함.
                entry.addListener(listener);

                // 남은 대기시간, ttl중 작은 값으로 타임아웃 시간 설정
                long t = time.get();
                if (ttl < time.get()) {
                    t = ttl;
                }
                if (!executed.get()) { // executed.get() == true -> 리스너가 실행됐다는 뜻. == false -> 리스너가 아직 실행되지 않았다면, 이라는 뜻.
                    // 아직 리스너가 실행되지 않았으니, t 만큼의 시간 후에 재시도를 예약한다는 의미.
                        // 즉, 락 해제 알림이 안와도 t 시간 이후에 강제로 재시도하는 로직임. 이게 없으면 락이 풀렸는데->알림못받은경우 waitTime까지 기다려야만 함. <-이 상황을 방지하기 위한 것.
                    // 리스너가 실행됐을 경우, 그 내부에서 tryLockAsync를 호출하므로 스케줄 생략.
                    Timeout scheduledFuture = commandExecutor.getServiceManager().newTimeout(timeout -> {
                        if (entry.removeListener(listener)) { // 타임아웃 로직 실행 이호 리스너를 제거해서 더이상 알림에 반응하지 않도록 한다.
                            long elapsed = System.currentTimeMillis() - current;
                            time.addAndGet(-elapsed);

                            tryLockAsync(time, waitTime, leaseTime, unit, entry, result, currentThreadId);
                        }
                    }, t, TimeUnit.MILLISECONDS);
                    futureRef.set(scheduledFuture);
                }
            }
        });
    }

    @Override
    <T> RFuture<T> tryLockInnerAsync(long waitTime, long leaseTime, TimeUnit unit, long threadId, RedisStrictCommand<T> command) {
        return commandExecutor.syncedEvalNoRetry(getRawName(), LongCodec.INSTANCE, command,
                "if ((redis.call('exists', KEYS[1]) == 0) " +
                        "or (redis.call('hexists', KEYS[1], ARGV[2]) == 1)) then " +
                            "redis.call('incr', KEYS[2]);" +
                            "redis.call('hincrby', KEYS[1], ARGV[2], 1); " +
                            "redis.call('pexpire', KEYS[1], ARGV[1]); " +
                            "return nil; " +
                        "end; " +
                        "return redis.call('pttl', KEYS[1]);",
                Arrays.asList(getRawName(), tokenName),
                unit.toMillis(leaseTime), getLockName(threadId));
    }

    @Override
    public RFuture<Boolean> deleteAsync() {
        return deleteAsync(getRawName(), tokenName);
    }

    @Override
    public RFuture<Long> sizeInMemoryAsync() {
        List<Object> keys = Arrays.asList(getRawName(), tokenName);
        return super.sizeInMemoryAsync(keys);
    }

    @Override
    public RFuture<Boolean> expireAsync(long timeToLive, TimeUnit timeUnit, String param, String... keys) {
        return super.expireAsync(timeToLive, timeUnit, param, getRawName(), tokenName);
    }

    @Override
    protected RFuture<Boolean> expireAtAsync(long timestamp, String param, String... keys) {
        return super.expireAtAsync(timestamp, param, getRawName(), tokenName);
    }

    @Override
    public RFuture<Boolean> clearExpireAsync() {
        return clearExpireAsync(getRawName(), tokenName);
    }

}
