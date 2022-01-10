<?php

namespace Heyday\Elastica;

use ArrayAccess;
use ArrayIterator;
use BadMethodCallException;
use Elastica\Index;
use Elastica\Query;
use Elastica\ResultSet;
use Exception;
use LogicException;
use Psr\Log\LoggerInterface;
use SilverStripe\ORM\ArrayList;
use SilverStripe\ORM\DataObject;
use SilverStripe\ORM\Limitable;
use SilverStripe\ORM\Map;
use SilverStripe\ORM\SS_List;
use SilverStripe\Versioned\Versioned;
use SilverStripe\View\ArrayData;
use SilverStripe\View\ViewableData;
use Traversable;

/**
 * A list wrapper around the results from a query. Note that not all operations are implemented.
 */
class ResultList extends ViewableData implements SS_List, Limitable
{
    /**
     * @var Index
     */
    private $index;
    /**
     * @var Query
     */
    private $query;
    private $logger;
    private $resultsArray;

    /**
     * @var ResultSet
     */
    private $resultSet;

    public function __construct(Index $index, Query $query, LoggerInterface $logger = null)
    {
        parent::__construct();

        //Optimise the query by just getting back the ids and types
        $query->setStoredFields([
            '_id',
            Searchable::TYPE_FIELD,
            'highlight'
        ]);

        //If we are in live reading mode, only return published documents
        if (Versioned::get_stage() == Versioned::LIVE) {
            $publishedFilter = new Query\BoolQuery();
            $publishedFilter->addMust(new Query\Term([Searchable::PUBLISHED_FIELD => true]));
            $query->setPostFilter($publishedFilter);
        }

        $this->index = $index;
        $this->query = $query;
        $this->logger = $logger;
    }

    /**
     *
     */
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
        /** @var $found Result[] */
        $found = $this->getResults();

        $ids = array();

        foreach ($found as $item) {
            $ids[] = $item->getId();
        }

        return $ids;
    }

    /**
     * @return Index
     */
    public function getIndex()
    {
        return $this->index;
    }

    /**
     * @return Query
     */
    public function getQuery()
    {
        return $this->query;
    }

    /**
     * @return array|ResultSet
     */
    public function getResults()
    {
        if (is_null($this->resultSet)) {
            try {
                $this->resultSet = $this->index->search($this->query);
            } catch (Exception $e) {
                if ($this->logger) {
                    $this->logger->critical($e->getMessage());
                }
            }
        }

        return $this->resultSet;
    }

    /**
     * @return ArrayIterator|Traversable
     */
    public function getIterator()
    {
        return new ArrayIterator($this->toArray());
    }

    /**
     * @param int $limit
     * @param int $offset
     * @return ResultList
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

            if (is_array($found) || $found instanceof ArrayAccess) {
                foreach ($found as $item) {
                    $type = isset($item->{Searchable::TYPE_FIELD}[0])
                      ? $item->{Searchable::TYPE_FIELD}[0]
                      : false;

                    if (empty($type)) {
                        throw new LogicException("type field not available");
                    }
                    if (!array_key_exists($type, $needed)) {
                        $needed[$type] = [$item->getId()];
                        $retrieved[$type] = [];
                    } else {
                        $needed[$type][] = $item->getId();
                    }
                }

                foreach ($needed as $class => $ids) {
                    foreach (DataObject::get($class)->byIDs($ids) as $record) {
                        $retrieved[$class][$record->ID] = $record;
                    }
                }

                foreach ($found as $item) {
                    // Safeguards against indexed items which might no longer be in the DB
                    $type = isset($item->{Searchable::TYPE_FIELD}[0])
                      ? $item->{Searchable::TYPE_FIELD}[0]
                      : false;

                    if (empty($type)) {
                        throw new LogicException("type field not available");
                    }

                    $id = $item->getId();

                    if (!isset($retrieved[$type][$id])) {
                        continue;
                    }

                    $highlights = $item->getHighlights();
                    $highlightsArray = [];

                    foreach ($highlights as $field => $highlight) {
                        $concatenatedValue = '';

                        foreach ($highlight as $key => $value) {
                            $concatenatedValue .= $value;
                        }

                        $highlightsArray[$field] = $concatenatedValue;
                    }

                    //add Highlights property
                    $retrieved[$type][$id]->highlights = new ArrayData($highlightsArray);
                    $this->resultsArray[] = $retrieved[$type][$id];
                }
            }
        }

        return $this->resultsArray;
    }

    /**
     * @return ArrayList
     */
    public function toArrayList()
    {
        return new ArrayList($this->toArray());
    }

    /**
     * @return array
     */
    public function toNestedArray()
    {
        $result = array();

        foreach ($this as $record) {
            $result[] = $record->toMap();
        }

        return $result;
    }

    /**
     * @return mixed
     */
    public function first()
    {
        $list = $this->toArray();
        return reset($list);
    }

    /**
     * @return mixed
     */
    public function last()
    {
        $list = $this->toArray();
        return array_pop($list);
    }


    /**
     * @param string $key
     * @param string $title
     * @return Map
     */
    public function map($key = 'ID', $title = 'Title')
    {
        return $this->toArrayList()->map($key, $title);
    }

    /**
     * @param string $col
     * @return array
     */
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

    /**
     * @param callable $callback
     * @return $this
     */
    public function each($callback)
    {
        $this->toArrayList()->each($callback);
        return $this;
    }

    /**
     * @return int
     */
    public function count(): int
    {
        return count($this->toArray());
    }

    /**
     * @return int
     */
    public function totalItems()
    {
        return $this->getResults()->getTotalHits();
    }

    /**
     * @inheritdoc
     */
    public function offsetExists($offset): bool
    {
        $array = $this->toArray();
        return array_key_exists($offset, $array);
    }

    /**
     * @inheritdoc
     */
    public function offsetGet($offset)
    {
        $array = $this->toArray();
        return isset($array[$offset]) ? $array[$offset] : null;
    }

    /**
     * @inheritdoc
     */
    public function find($key, $value)
    {
        return $this->toArrayList()->find($key, $value);
    }

    /**
     * @inheritdoc
     */
    public function offsetSet($offset, $value): void
    {
        throw new BadMethodCallException("ResultList cannot be modified in memory");
    }

    /**
     * @inheritdoc
     */
    public function offsetUnset($offset): void
    {
        throw new BadMethodCallException("ResultList cannot be modified in memory");
    }

    /**
     * @inheritdoc
     */
    public function add($item)
    {
        throw new BadMethodCallException("ResultList cannot be modified in memory");
    }

    /**
     * @inheritdoc
     */
    public function remove($item)
    {
        throw new BadMethodCallException("ResultList cannot be modified in memory");
    }
}
