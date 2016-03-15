<?php

namespace Heyday\Elastica;

use Elastica\Index;
use Elastica\Query;
use Elastica\ResultSet;
use Psr\Log\LoggerInterface;

/**
 * A list wrapper around the results from a query. Note that not all operations are implemented.
 */
class ResultList extends \ViewableData implements \SS_Limitable
{

    private $index;
    private $query;
    private $logger;
    private $resultsArray;

    /**
     * @var ResultSet
     */
    private $resultSet;

    public function __construct(Index $index, Query $query, LoggerInterface $logger = null)
    {
        //Optimise the query by just getting back the ids and types
        $query->setFields(array(
            '_id',
            '_type'
        ));

        //If we are in live reading mode, only return published documents
        if (\Versioned::get_reading_mode() == \Versioned::DEFAULT_MODE) {
            $publishedFilter = new Query\BoolQuery();
            $publishedFilter->addMust(new Query\Term([Searchable::$published_field => 'true']));
            $query->setPostFilter($publishedFilter);
        }

        $this->index = $index;
        $this->query = $query;
        $this->logger = $logger;
    }

    public function __clone()
    {
        $this->query = clone $this->query;
        $this->resultsArray = false;
        $this->resultSet = null;
    }

    /**
     * Get array of IDs of the results
     * @return array
     */
    public function getIDs()
    {
        /** @var $found \Elastica\Result[] */
        $found = $this->getResults();

        $ids = array();

        foreach ($found as $item) {
            $ids[] = $item->getId();
        }

        return $ids;
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
     * @return array|\Elastica\ResultSet
     */
    public function getResults()
    {
        if (is_null($this->resultSet)) {
            try {
                $this->resultSet = $this->index->search($this->query);
            } catch (\Exception $e) {
                if ($this->logger) {
                    $this->logger->critical($e->getMessage());
                }
            }
        }

        return $this->resultSet;
    }

    /**
     * @return \ArrayIterator|\Traversable
     */
    public function getIterator()
    {
        return new \ArrayIterator($this->toArray());
    }

    /**
     * @param $limit
     * @param int $offset
     * @return ResultList|\SS_Limitable
     */
    public function limit($limit, $offset = 0)
    {
        $list = clone $this;

        $list->getQuery()->setSize($limit);
        $list->getQuery()->setFrom($offset);

        return $list;
    }

    /**
     * @param array $sortArgs
     * @return ResultList
     */
    public function sort(array $sortArgs)
    {
        $list = clone $this;

        $list->getQuery()->setSort($sortArgs);

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
        if (!is_array($this->resultsArray)) {

            $this->resultsArray = array();

            $found = $this->getResults();
            $needed = array();
            $retrieved = array();

            if (is_array($found) || $found instanceof \ArrayAccess) {

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
                        $this->resultsArray[] = $retrieved[$item->getType()][$item->getId()];
                    }
                }
            }
        }

        return $this->resultsArray;
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
        return reset($this->toArray());
    }

    public function last()
    {
        return array_pop($this->toArray());
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

    public function totalItems()
    {
        return $this->getResults()->getTotalHits();
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
