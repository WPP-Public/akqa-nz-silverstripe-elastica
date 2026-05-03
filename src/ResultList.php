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
use SilverStripe\Core\Injector\Injector;
use SilverStripe\Model\ArrayData;
use SilverStripe\Model\List\ArrayList;
use SilverStripe\Model\List\Map;
use SilverStripe\Model\List\SS_List;
use SilverStripe\Model\ModelData;
use SilverStripe\ORM\DataObject;
use SilverStripe\ORM\FieldType\DBField;
use SilverStripe\Versioned\Versioned;
use Traversable;

/**
 * A list wrapper around the results from a query. Note that not all operations are implemented.
 *
 * @template T of DataObject
 * @implements SS_List<T>
 */
class ResultList extends ModelData implements SS_List
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

    public function __construct(Index $index, Query $query, LoggerInterface $logger)
    {
        parent::__construct();

        //Optimise the query by just getting back the ids and types
        $query->setStoredFields(
            [
            '_id',
            Searchable::TYPE_FIELD,
            ]
        );

        if (Versioned::get_reading_mode() == Versioned::LIVE) {
            $publishedFilter = $query->hasParam('post_filter') ? $query->getParam('post_filter') : null;

            if (!$publishedFilter) {
                $publishedFilter = new Query\BoolQuery();
            } elseif (!($publishedFilter instanceof Query\BoolQuery)) {
                throw new \RuntimeException("Please use a bool query for your post_filter");
            }

            $publishedFilter->addMust(new Query\Term([Searchable::PUBLISHED_FIELD => 'true']));
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
     *
     * @return string[]
     */
    public function getIDs()
    {
        $found = $this->getResults();

        $ids = [];

        if ($found instanceof ResultSet) {
            foreach ($found as $item) {
                $ids[] = $item->getId();
            }
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
     * @return ResultSet|null
     */
    public function getResults()
    {
        if (is_null($this->resultSet)) {
            try {
                $this->resultSet = $this->index->search($this->query);
            } catch (Exception $e) {
                if ($this->logger) {
                    $this->logger->error('Elasticsearch query failed: ' . $e->getMessage(), [
                        'exception' => $e,
                        'query' => json_encode($this->query->toArray()),
                    ]);
                } else {
                    // Re-throw if no logger to avoid silent failures
                    throw $e;
                }
            }
        }

        return $this->resultSet;
    }

    /**
     * @return Traversable
     */
    public function getIterator(): Traversable
    {
        return new ArrayIterator($this->toArray());
    }

    /**
     * @param int|null $length
     * @param int $offset
     * @return SS_List<T>
     */
    public function limit(?int $length, int $offset = 0): SS_List
    {
        $list = clone $this;

        $list->getQuery()->setSize($length);
        $list->getQuery()->setFrom($offset);

        return $list;
    }

    /**
     * @param mixed ...$args
     * @return SS_List<T>
     */
    public function sort(...$args): SS_List
    {
        $list = clone $this;

        // Handle both array and flexible arguments
        if (count($args) === 1 && is_array($args[0])) {
            $sortArgs = $args[0];
        } else {
            $sortArgs = $args;
        }

        $list->getQuery()->setSort($sortArgs);

        return $list;
    }


    /**
     * Converts results of type {@link \Elastica\Result}
     * into their respective {@link DataObject} counterparts.
     *
     * @return array<T>
     */
    public function toArray(): array
    {
        if (!is_array($this->resultsArray)) {
            $this->resultsArray = array();

            $found = $this->getResults();
            $needed = array();
            $retrieved = array();

            if ($found instanceof ResultSet && $found->count() === 0) {
                Injector::inst()->get(LoggerInterface::class)
                    ->debug('ResultList::toArray() - Elasticsearch returned 0 results');
            }

            if ($found instanceof ResultSet || $found instanceof ArrayAccess || is_array($found)) {
                foreach ($found as $item) {
                    $type = isset($item->{Searchable::TYPE_FIELD}[0])
                      ? $item->{Searchable::TYPE_FIELD}[0]
                      : false;

                    if (empty($type)) {
                        Injector::inst()->get(LoggerInterface::class)->error(
                            'No type field found on result: '. $item->getId()
                        );

                        continue;
                    }

                    if (!array_key_exists($type, $needed)) {
                        $needed[$type] = [$item->getId()];

                        $retrieved[$type] = [];
                    } else {
                        $needed[$type][] = $item->getId();
                    }
                }

                foreach ($needed as $class => $documentIds) {
                    // Skip if the class doesn't exist (stale index data or invalid type)
                    if (!class_exists($class)) {
                        Injector::inst()->get(LoggerInterface::class)
                            ->warning("ResultList: Class '$class' from Elasticsearch index does not exist, skipping");
                        continue;
                    }

                    // Skip if the class is not a DataObject subclass
                    if (!is_subclass_of($class, DataObject::class)) {
                        Injector::inst()->get(LoggerInterface::class)
                            ->warning("ResultList: Class '$class' is not a DataObject subclass, skipping");
                        continue;
                    }

                    $ids = array_map(function ($documentId) {
                        $parts = preg_split('/_/', $documentId);

                        return end($parts);
                    }, $documentIds);

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
                        continue;
                    }

                    $documentId = $item->getId();
                    $parts = preg_split('/_/', $documentId);
                    $id = end($parts);

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

                        $highlightsArray[$field] = DBField::create_field('HTMLText', $concatenatedValue);
                    }

                    $retrieved[$type][$id]->highlights = new ArrayData($highlightsArray);

                    $this->resultsArray[] = $retrieved[$type][$id];
                }
            }
        }

        return $this->resultsArray;
    }

    /**
     * @return ArrayList<T>
     */
    public function toArrayList(): ArrayList
    {
        return new ArrayList($this->toArray());
    }

    /**
     * @return array
     */
    public function toNestedArray(): array
    {
        $result = array();

        foreach ($this as $record) {
            $result[] = $record->toMap();
        }

        return $result;
    }


    public function getFirstItem()
    {
        try {
            $from = $this->getQuery()->getParam('from');
        } catch (Exception $e) {
            $from = 1;
        }

        return $from;
    }


    public function getLastItem()
    {
        try {
            $start = $this->getFirstItem();
            $to = min($start + $this->getQuery()->getParam('size'), $this->getTotalItems());
        } catch (Exception $e) {
            $to = min(10, $this->getTotalItems());
        }

        return $to;
    }


    /**
     * @return T|null
     */
    public function first(): mixed
    {
        $list = $this->toArray();
        return reset($list) ?: null;
    }

    /**
     * @return T|null
     */
    public function last(): mixed
    {
        $list = $this->toArray();
        return array_pop($list);
    }


    /**
     * @param string $keyfield
     * @param string $titlefield
     * @return Map
     */
    public function map(string $keyfield = 'ID', string $titlefield = 'Title'): Map
    {
        return $this->toArrayList()->map($keyfield, $titlefield);
    }

    /**
     * @param string $colName
     * @return array
     */
    public function column(string $colName = 'ID'): array
    {
        if ($colName == 'ID') {
            $ids = array();
            $results = $this->getResults();

            if ($results) {
                foreach ($results as $result) {
                    $ids[] = $result->getId();
                }
            }

            return $ids;
        } else {
            return $this->toArrayList()->column($colName);
        }
    }

    /**
     * @param string $colName
     * @return array
     */
    public function columnUnique(string $colName = 'ID'): array
    {
        return array_unique($this->column($colName));
    }

    /**
     * @param callable $callback
     * @return SS_List<T>
     */
    public function each(callable $callback): SS_List
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
    public function getTotalItems()
    {
        $results = $this->getResults();
        return $results ? $results->getTotalHits() : 0;
    }

    /**
     * @return bool
     */
    public function exists(): bool
    {
        return $this->count() > 0;
    }

    /**
     * @inheritdoc
     */
    public function offsetExists(mixed $offset): bool
    {
        $array = $this->toArray();
        return array_key_exists($offset, $array);
    }

    /**
     * @inheritdoc
     */
    public function offsetGet(mixed $offset): mixed
    {
        $array = $this->toArray();
        return isset($array[$offset]) ? $array[$offset] : null;
    }

    /**
     * @param string $key
     * @param mixed $value
     * @return T|null
     */
    public function find(string $key, mixed $value): mixed
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
    public function add(mixed $item): void
    {
        throw new BadMethodCallException("ResultList cannot be modified in memory");
    }

    /**
     * @inheritdoc
     */
    public function remove(mixed $item)
    {
        throw new BadMethodCallException("ResultList cannot be modified in memory");
    }

    /**
     * @param string $by
     * @return bool
     */
    public function canFilterBy(string $by): bool
    {
        // Elasticsearch can filter by any indexed field
        return true;
    }

    /**
     * @param string $by
     * @return bool
     */
    public function canSortBy(string $by): bool
    {
        // Elasticsearch can sort by any indexed field
        return true;
    }

    /**
     * @param mixed ...$args
     * @return SS_List<T>
     */
    public function filter(...$args): SS_List
    {
        // Convert to ArrayList and filter there
        return $this->toArrayList()->filter(...$args);
    }

    /**
     * @param mixed ...$args
     * @return SS_List<T>
     */
    public function filterAny(...$args): SS_List
    {
        return $this->toArrayList()->filterAny(...$args);
    }

    /**
     * @param mixed ...$args
     * @return SS_List<T>
     */
    public function exclude(...$args): SS_List
    {
        return $this->toArrayList()->exclude(...$args);
    }

    /**
     * @param mixed ...$args
     * @return SS_List<T>
     */
    public function excludeAny(...$args): SS_List
    {
        return $this->toArrayList()->excludeAny(...$args);
    }

    /**
     * @param callable $callback
     * @return SS_List<T>
     */
    public function filterByCallback(callable $callback): SS_List
    {
        return $this->toArrayList()->filterByCallback($callback);
    }

    /**
     * @param mixed $id
     * @return T|null
     */
    public function byID(mixed $id): mixed
    {
        return $this->toArrayList()->byID($id);
    }

    /**
     * @param array $ids
     * @return SS_List<T>
     */
    public function byIDs(array $ids): SS_List
    {
        return $this->toArrayList()->byIDs($ids);
    }

    /**
     * @return SS_List<T>
     */
    public function reverse(): SS_List
    {
        return $this->toArrayList()->reverse();
    }
}
