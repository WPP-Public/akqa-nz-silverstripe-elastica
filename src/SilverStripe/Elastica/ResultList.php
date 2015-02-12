<?php

namespace SilverStripe\Elastica;

use Elastica\Index;
use Elastica\Query;

/**
 * A list wrapper around the results from a query. Note that not all operations are implemented.
 */
class ResultList extends \ViewableData implements \SS_Limitable, \SS_List
{

    private $index;
    private $query;

    public function __construct(Index $index, Query $query)
    {
        $this->index = $index;
        $this->query = $query;
    }

    public function __clone()
    {
        $this->query = clone $this->query;
    }

    /**
     * @return \Elastica\Index
     */
    public function getIndex()
    {
        return $this->index;
    }

    /**
     * @return \Elastica\Query
     */
    public function getQuery()
    {
        return $this->query;
    }

    /**
     * @return array
     */
    public function getResults()
    {
        return $this->index->search($this->query)->getResults();
    }

    public function getIterator()
    {
        return new \ArrayIterator($this->toArray());
    }

    public function limit($limit, $offset = 0)
    {
        $list = clone $this;

        $list->getQuery()->setLimit($limit);
        $list->getQuery()->setFrom($offset);

        return $list;
    }

    /**
     * Converts results of type {@link \Elastica\Result}
     * into their respective {@link DataObject} counterparts.
     *
     * @return array DataObject[]
     */
    public function toArray()
    {
        $result = array();

        /** @var $found \Elastica\Result[] */
        $found = $this->getResults();
        $needed = array();
        $retrieved = array();

        foreach ($found as $item) {
            $type = $item->getType();

            if (!array_key_exists($type, $needed)) {
                $needed[$type] = array($item->getId());
                $retrieved[$type] = array();
            } else {
                $needed[$type][] = $item->getId();
            }
        }

        foreach ($needed as $class => $ids) {
            foreach ($class::get()->byIDs($ids) as $record) {
                $retrieved[$class][$record->ID] = $record;
            }
        }

        foreach ($found as $item) {
            // Safeguards against indexed items which might no longer be in the DB
            if (array_key_exists($item->getId(), $retrieved[$item->getType()])) {
                $result[] = $retrieved[$item->getType()][$item->getId()];
            }
        }

        return $result;
    }

    public function toArrayList()
    {
        return new \ArrayList($this->toArray());
    }

    public function toNestedArray()
    {
        $result = array();

        foreach ($this as $record) {
            $result[] = $record->toMap();
        }

        return $result;
    }

    public function first()
    {
        // TODO
    }

    public function last()
    {
        // TODO: Implement last() method.
    }


    public function map($key = 'ID', $title = 'Title')
    {
        return $this->toArrayList()->map($key, $title);
    }

    public function column($col = 'ID')
    {
        if ($col == 'ID') {
            $ids = array();

            foreach ($this->getResults() as $result) {
                $ids[] = $result->getId();
            }

            return $ids;
        } else {
            return $this->toArrayList()->column($col);
        }
    }

    public function each($callback)
    {
        return $this->toArrayList()->each($callback);
    }

    public function count()
    {
        return count($this->toArray());
    }

    /**
     * @ignore
     */
    public function offsetExists($offset)
    {
        throw new \Exception();
    }

    /**
     * @ignore
     */
    public function offsetGet($offset)
    {
        throw new \Exception();
    }

    /**
     * @ignore
     */
    public function offsetSet($offset, $value)
    {
        throw new \Exception();
    }

    /**
     * @ignore
     */
    public function offsetUnset($offset)
    {
        throw new \Exception();
    }

    /**
     * @ignore
     */
    public function add($item)
    {
        throw new \Exception();
    }

    /**
     * @ignore
     */
    public function remove($item)
    {
        throw new \Exception();
    }

    /**
     * @ignore
     */
    public function find($key, $value)
    {
        throw new \Exception();
    }

}