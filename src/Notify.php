<?php

namespace Redpot;

class Notify
{

    /**
     *
     * @var int
     */
    private $userId = 0;

    /**
     *
     * @var \Redis
     */
    private $redis;

    private $keyPrefix = [];

    /**
     *
     * @param \Redis $redis
     */
    public function __construct(\Redis $redis,
        string $keyPrefixGlobal = 'redpot/global',
        string $keyPrefixPerson = 'redpot/person',
        string $keyPrefixMarked = 'redpot/marked')
    {
        $this->redis = $redis;

        $this->keyPrefix['global'] = $keyPrefixGlobal;
        $this->keyPrefix['person'] = $keyPrefixPerson;
        $this->keyPrefix['marked'] = $keyPrefixMarked;
    }

    public function setUserId($userId): self {
        $this->userId = $userId;
        return $this;
    }

    /**
     *
     * @param string $name
     * @param int $version
     * @return bool|int
     */
    public function setGlobal(string $name, int $version = null) {
        $key = $this->makeGlobalKey();
        return $this->save($key, $name, $version);
    }

    /**
     *
     * @param string $name
     * @param int $version
     * @return bool|int
     */
    public function set(string $name, int $version = null) {
        $key = $this->makePersonKey();
        return $this->save($key, $name, $version);
    }

    public function delGlobal(string $name) {
        $key = $this->makeGlobalKey();
        return $this->redis->hDel($key, $name);
    }

    public function del(string $name) {
        $key = $this->makePersonKey();
        return $this->redis->hDel($key, $name);
    }

    public function clean(): self {
        $this->redis->del($this->makePersonKey());
        $this->redis->del($this->makeMarkedKey());
        return $this;
    }

    private function packAggs(array $aggs, array $data): array {
        $keys = array_keys($data);
        foreach ($aggs as $name => $delimiter) {
            $first  = $name.$delimiter;
            $length = strlen($first);
            foreach ($keys as $key) {
                if (($pos = strpos($key, $first)) !== 0) {
                    continue;
                }
                $data[$name][substr($key, $length)] = $data[$key];
                unset($data[$key]);
            }
        }
        return $data;
    }

    private function unpackAggs(array $aggs, array $data): array {
        $agged = [];
        foreach ($data as $key => $value) {
            if (is_array($value)) {
                unset($data[$key]);
                if (!isset($aggs[$key])) {
                    continue;
                }
                foreach ($value as $k => $v) {
                    $data[$key.$aggs[$key].$k] = $v;
                }
            }
        }
        return $data;
    }

    /**
     *
     * @param array $marked
     * @param array $aggs
     * @return array
     */
    public function __invoke(array $marked = [], array $aggs = []): array {
        $aggs && $marked = $this->unpackAggs($aggs, $marked);

        $result_newest  = [];

        // 合并获得最新notify信息
        $global = $this->redis->hGetAll($this->makeGlobalKey()) ?: [];
        $person = $this->redis->hGetAll($this->makePersonKey()) ?: [];
        $newest = $this->mergeNewest($global, $person);

        // 用户marked数据是否需要初始化
        $is_init = false;
        $marked_key     = $this->makeMarkedKey();
        $marked_saved   = $this->redis->hGetAll($marked_key);
        if (!$marked_saved && !$this->redis->exists($marked_key)) {
            // 用户无数据，进行初始化
            $marked_saved = $newest;
            $is_init = true;
        }

        // 同步客户端的红点状态到服务端
        $marked_updates = [];
        foreach ($marked as $name => $version) {
            // 这里仍然会以提交数据为准，即使版本号有倒退
            // 非0版本才同步
            if (!$version ||
                (isset($marked_saved[$name]) && $marked_saved[$name] == $version)) {
                continue;
            }
            $marked_saved[$name] = $marked_updates[$name] = $version;
        }

        // 更新marked
        // 需要初始化时更新全量
        $result_updated = $is_init ? $marked_saved : $marked_updates;
        $result_updated && $this->updateMarked($result_updated);

        // 匹配全局
        foreach ($this->match($marked_saved, $newest)
            as $name => $version) {
            $result_newest[$name] = $version;
        }

        $aggs && $result_newest  = $this->packAggs($aggs, $result_newest);
        $aggs && $result_updated = $this->packAggs($aggs, $result_updated);
        return [$result_newest, $result_updated];
    }

    /**
     *
     * @param array $merges
     * @return array
     */
    private function mergeNewest(...$merges): array {
        // 合并多个维度的notify数据
        $merged = [];
        foreach ($merges as $list) {
            foreach ($list as $name => $version) {
                $merged[$name] = isset($merged[$name]) ?
                    ($merged[$name] + $version) : $version;
            }
        }
        return $merged;
    }

    /**
     *
     *
     * @param array $marked
     * @param array $merged
     */
    private function match(array $marked, $merged) {
        // 一次性做版本校验
        foreach ($merged as $name => $version) {
            // 不存在的键值总是抛出
            if (!isset($marked[$name])) {
                yield $name => $version;
            } elseif ($marked[$name] != $version) { // id不对即有提示，即使mk更大也一样
                yield $name => $version;
            }
        }
    }

    /**
     * @todo 校正的写入代码可以使用multi()方法批量处理
     *
     * @param array $list
     */
    private function updateMarked(array $list) {
        $key = $this->makeMarkedKey();
        foreach ($list as $name => $version) {
            $this->save($key, $name, $version);
        }
    }

    /**
     *
     * @param string $key
     * @param string $name
     * @param int $version
     * @return bool|int
     */
    private function save(string $key, string $name, int $version = null) {
        if ($version === null) {
            $previous = $this->redis->hGet($key, $name) ?: 0;
            $version = $previous + 1;
        }
        return $this->redis->hSet($key, $name, $version);
    }

    /**
     * 创建全局 hash key
     *
     * @return string
     */
    private function makeGlobalKey(): string {
        return $this->keyPrefix['global'];
    }

    /**
     * 创建个人 hash key
     * @return string
     */
    private function makePersonKey(): string {
        return "{$this->keyPrefix['person']}/$this->userId";
    }

    /**
     * 创建个人标记 hash key
     * @return string
     */
    private function makeMarkedKey(): string {
        return "{$this->keyPrefix['marked']}/$this->userId";
    }
}