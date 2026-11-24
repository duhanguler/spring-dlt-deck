-- Atomic token-bucket rate limiter for multi-pod spring-dlt-deck replay coordination.
-- KEYS[1] = bucket hash key
-- ARGV[1] = permitsPerSecond (float)
-- ARGV[2] = maxBurst (float)
-- ARGV[3] = nowMillis (integer)
-- ARGV[4] = requestedPermits (float)
-- ARGV[5] = ttlSeconds (integer)
-- Returns {allowed (1|0), waitMillis (integer)}

local key = KEYS[1]
local rate = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local now_ms = tonumber(ARGV[3])
local requested = tonumber(ARGV[4])
local ttl_sec = tonumber(ARGV[5])

local data = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local last_ms = tonumber(data[2])

if tokens == nil or last_ms == nil then
    tokens = capacity
    last_ms = now_ms
else
    local elapsed_ms = math.max(0, now_ms - last_ms)
    local refilled = (elapsed_ms / 1000.0) * rate
    tokens = math.min(capacity, tokens + refilled)
    last_ms = now_ms
end

local allowed = 0
local wait_ms = 0

if tokens >= requested then
    tokens = tokens - requested
    allowed = 1
else
    local deficit = requested - tokens
    wait_ms = math.ceil((deficit / rate) * 1000.0)
end

redis.call('HMSET', key, 'tokens', string.format('%.6f', tokens), 'ts', tostring(last_ms))
redis.call('EXPIRE', key, ttl_sec)

return {allowed, wait_ms}
