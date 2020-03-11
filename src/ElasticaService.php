<?php

namespace Heyday\Elastica;

use Elastica\Client;
use Elastica\Document;
use Elastica\Exception\NotFoundException;
use Elastica\Index;
use Elastica\Query;
use Elastica\Response;
use Exception;
use InvalidArgumentException;
use LogicException;
use Psr\Log\LoggerInterface;
use SilverStripe\Control\Director;
use SilverStripe\Core\ClassInfo;
use SilverStripe\Core\Config\Config;
use SilverStripe\Core\Config\Configurable;
use SilverStripe\ORM\DataObject;
use SilverStripe\Versioned\Versioned;

/**
 * A service used to interact with elastic search.
 */
class ElasticaService
{
    use Configurable;

    const CONFIGURE_DISABLE_INDEXING = 'disable_indexing';

    /**
     * @var Client
     */
    private $client;

    /**
     * @var string
     */
    private $indexName;

    /**
     * @var LoggerInterface
     */
    private $logger;

    /**
     * @var string
     */
    private $indexingMemory;

    /**
     * @var bool
     */
    private $indexingMemorySet = false;

    public $searchableExtensionClassName;

    /**
     * Unprocessed batch operations.
     * Many-depth array:
     *  - First level is batch depth (e.g. nested batching)
     *  - Second level is associative array of types
     *  - Third level is a pair of keys 'index' (add/update) and 'delete' (remove)
     *  - Fourth level is the list of documents to index / delete
     *
     * @var Document[][][][]
     */
    protected $batches = [];

    const UPDATES = 'updates';

    const DELETES = 'deletes';

    /**
     * ElasticaService constructor.
     *
     * @param Client               $client
     * @param string               $indexName
     * @param LoggerInterface|null $logger Increases the memory limit while indexing.
     * @param string               $indexingMemory A memory limit string, such as "64M".
     * @param string               $searchableExtensionClassName
     */
    public function __construct(
        Client $client,
        $indexName,
        LoggerInterface $logger = null,
        $indexingMemory = null,
        $searchableExtensionClassName = Searchable::class
    ) {
        $this->client = $client;
        $this->indexName = $indexName;
        $this->logger = $logger;
        $this->indexingMemory = $indexingMemory;
        $this->searchableExtensionClassName = $searchableExtensionClassName;
    }

    /**
     * @return Client
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @return Index
     */
    public function getIndex()
    {
        return $this->getClient()->getIndex($this->indexName);
    }

    /**
     * @return array
     */
    protected function getIndexConfig()
    {
        return $this->config()->get('index_config');
    }

    /**
     * Performs a search query and returns either a ResultList (SS template compatible) or an Elastica\ResultSet
     * @param \Elastica\Query|string|array $query
     * @param array                        $options Options defined in \Elastica\Search
     * @param bool                         $returnResultList
     * @return ResultList | \Elastica\ResultSet
     */
    public function search($query, $options = null, $returnResultList = true)
    {
        if ($returnResultList) {
            return new ResultList($this->getIndex(), Query::create($query), $this->logger);
        }
        return $this->getIndex()->search($query, $options);
    }

    /**
     * @throws Exception
     */
    public function createIndex()
    {
        $index = $this->getIndex();

        if ($config = $this->getIndexConfig()) {
            try {
                $index->create($config, true);
            } catch (Exception $e) {
                $this->exception($e);
            }
        } else {
            $index->create();
        }
    }

    /**
     * Remove the index
     *
     * @throws Exception
     */
    public function deleteIndex()
    {
        $index = $this->getIndex();
        try {
            $index->delete();
        } catch (Exception $e) {
            $this->exception($e);
        }
    }

    /**
     * Either creates or updates a record in the index.
     *
     * @param Searchable|DataObject $record
     * @return Response|null|bool Return response, or true if batched
     * @throws Exception
     */
    public function index($record)
    {
        // Ignore if disabled or only a supporting type
        if ($this->config()->get(self::CONFIGURE_DISABLE_INDEXING) || $record->config()->get('supporting_type')) {
            return null;
        }

        if (!$this->indexingMemorySet && $this->indexingMemory) {
            if ($this->indexingMemory == 'unlimited') {
                ini_set('memory_limit', -1);
            } else {
                ini_set('memory_limit', $this->indexingMemory);
            }
            $this->indexingMemorySet = true;
        }

        try {
            $document = $record->getElasticaDocument();
            $typeName = $record->getElasticaType();
            $index = $this->getIndex();

            // If batching
            if ($this->isBatching()) {
                $this->batchDocument($typeName, self::UPDATES, $document);
                return true;
            }

            $type = $index->getType($typeName);
            $response = $type->addDocument($document);
            $index->refresh();

            return $response;
        } catch (Exception $e) {
            $this->exception($e);
            return null;
        }
    }

    /**
     * Detect if we are batching queries
     *
     * @return bool
     */
    protected function isBatching()
    {
        return !empty($this->batches);
    }

    /**
     * Pause all add / remove operations, batching these at the completion of a user-provided callback.
     * For example, you might call batch with a closure that initiates ->index() on 20 records.
     * On the conclusion of this closure, those 20 updates will be batched together into a single update
     *
     * @param callable $callback Callback within which to batch updates
     * @param int      $documentsProcessed Number of documents processed during this batch
     * @return mixed result of $callback
     * @throws Exception
     */
    public function batch(callable $callback, &$documentsProcessed = 0)
    {
        try {
            $this->batches[] = []; // Increase batch depth one level
            return $callback();
        } finally {
            try {
                $batch = array_pop($this->batches);
                $documentsProcessed = $this->flushBatch($batch);
            } catch (Exception $ex) {
                $this->exception($ex);
            }
        }
    }

    /**
     * Process a batch update
     *
     * @param Document[][][] $batch List of updates for this batch, grouped by type
     * @return int Number of documents updated in this batch
     */
    protected function flushBatch($batch)
    {
        $documentsProcessed = 0;

        // process batches
        $index = null;
        foreach ($batch as $type => $changes) {
            $typeObject = null;
            foreach ($changes as $action => $documents) {
                if (empty($documents)) {
                    continue;
                }
                $index = $index ?: $this->getIndex();
                $typeObject = $typeObject ?: $index->getType($type);
                $documentsProcessed += count($documents);
                switch ($action) {
                    case self::UPDATES:
                        $typeObject->addDocuments($documents);
                        break;
                    case self::DELETES:
                        try {
                            $typeObject->deleteDocuments($documents);
                        } catch (NotFoundException $ex) {
                            // no-op if not found
                        }
                        break;
                    default:
                        throw new LogicException("Invalid batch action {$action}");
                }
            }
        }
        // Refresh if any documents updated
        if ($documentsProcessed && $index) {
            $index->refresh();
        }
        return $documentsProcessed;
    }

    /**
     * Add document to batch query
     *
     * @param string   $type elasticsearch type name
     * @param string   $action self::DELETES or self::UPDATES
     * @param Document $document
     */
    protected function batchDocument($type, $action, $document)
    {
        if (!is_string($type)) {
            throw new InvalidArgumentException("Invalid type argument");
        }
        if (!is_string($action) || !in_array($action, [self::DELETES, self::UPDATES])) {
            throw new InvalidArgumentException("Invalid action argument");
        }
        $batchIndex = count($this->batches) - 1;
        // Ensure keys exist
        if (!isset($this->batches[$batchIndex][$type])) {
            $this->batches[$batchIndex][$type] = [];
        }
        // Ensure that DELETES occur before UPDATES in all instances
        if (!isset($this->batches[$batchIndex][$type][self::DELETES])) {
            $this->batches[$batchIndex][$type][self::DELETES] = [];
        }
        if (!isset($this->batches[$batchIndex][$type][self::UPDATES])) {
            $this->batches[$batchIndex][$type][self::UPDATES] = [];
        }
        // Add document
        $this->batches[$batchIndex][$type][$action][] = $document;
    }

    /**
     * @param Searchable|DataObject $record
     * @return Response|null|bool Response, or true if batched
     * @throws Exception
     */
    public function remove($record)
    {
        // Ignore if disabled or only a supporting type
        if ($this->config()->get(self::CONFIGURE_DISABLE_INDEXING) || $record->config()->get('supporting_type')) {
            return null;
        }

        try {
            $index = $this->getIndex();
            $typeName = $record->getElasticaType();
            $document = $record->getElasticaDocument();
            // If batching
            if ($this->isBatching()) {
                $this->batchDocument($typeName, self::DELETES, $document);
                return true;
            }

            $type = $index->getType($typeName);
            return $type->deleteDocument($document);
        } catch (NotFoundException $ex) {
            // If deleted records already were deleted, treat as non-error
            return null;
        } catch (Exception $e) {
            $this->exception($e);
            return null;
        }
    }

    /**
     * Creates the index and the type mappings.
     *
     * @param bool $recreate
     * @throws Exception
     */
    public function define($recreate = false)
    {
        $index = $this->getIndex();

        if ($index->exists() && $recreate) {
            // Delete the existing index so it can be recreated from scratch
            $index->delete();
        }

        if (!$index->exists()) {
            $this->createIndex();
        }

        foreach ($this->getIndexedClasses() as $class) {
            /** @var $sng Searchable */
            $sng = singleton($class);

            $mapping = $sng->getElasticaMapping();
            if ($mapping) {
                $mapping->setType($index->getType($sng->getElasticaType()));
                $mapping->send();
            }
        }
    }

    /**
     * Re-indexes each record in the index.
     * @throws Exception
     */
    public function refresh()
    {
        $reading_mode = Versioned::get_reading_mode();
        Versioned::set_reading_mode('Stage.Live');

        foreach ($this->getIndexedClasses() as $class) {
            //Only index types (or classes) that are not just supporting other index types
            if (!Config::inst()->get($class, 'supporting_type')) {
                /** @var DataObject $record */
                foreach ($class::get() as $record) {
                    // Only index records with Show In Search enabled, or those that don't expose that fielid
                    if (!$record->hasField('ShowInSearch') || $record->ShowInSearch) {
                        if ($this->index($record)) {
                            $this->printActionMessage($record, 'INDEXED');
                        }
                    } else {
                        if ($this->remove($record)) {
                            $this->printActionMessage($record, 'REMOVED');
                        }
                    }
                }
            }
        }
        Versioned::set_reading_mode($reading_mode);
    }

    /**
     * Gets the classes which are indexed (i.e. have the extension applied).
     *
     * @return array
     */
    public function getIndexedClasses()
    {
        $classes = array();
        foreach (ClassInfo::subclassesFor(DataObject::class) as $candidate) {
            $candidateInstance = DataObject::singleton($candidate);
            if ($candidateInstance->hasExtension($this->searchableExtensionClassName)) {
                $classes[] = $candidate;
            }
        }
        return $classes;
    }

    /**
     * Output message when item is indexed / removed
     *
     * @param DataObject $record
     * @param string     $action Action type
     */
    protected function printActionMessage(DataObject $record, $action)
    {
        $documentDetails = "Document Type \"{$record->ClassName}\" - {$record->Title} - ID {$record->ID}";
        if (Director::is_cli()) {
            print "{$action}: {$documentDetails}\n";
        } else {
            print "<strong>{$action}: </strong>{$documentDetails}<br>";
        }
    }

    /**
     * If a logger is configured, log the exception there.
     * Otherwise the exception is thrown
     *
     * @param Exception $exception
     * @throws Exception
     */
    protected function exception($exception)
    {
        // If no logger specified expose error normally
        if (!$this->logger) {
            throw $exception;
        }

        $message = sprintf(
            'Uncaught Exception %s: "%s" at %s line %s',
            get_class($exception),
            $exception->getMessage(),
            $exception->getFile(),
            $exception->getLine()
        );
        $this->logger->error($message);
    }
}
