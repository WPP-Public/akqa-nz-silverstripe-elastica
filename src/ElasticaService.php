<?php

namespace Heyday\Elastica;

use Elastica\Client;
use Elastica\Exception\NotFoundException;
use Elastica\Query;
use Psr\Log\LoggerInterface;

/**
 * A service used to interact with elastic search.
 */
class ElasticaService extends \Object
{

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

    /**
     * @param Client $client
     * @param string $indexName
     * @param LoggerInterface $logger
     */
    /**
     * @param Client $client
     * @param $indexName
     * @param LoggerInterface $logger
     * @param string $indexingMemory Increases the memory limit while indexing. A memory limit string, such as "64M".
     * 'unlimited' if you want no limit
     */
    public function __construct(Client $client, $indexName, LoggerInterface $logger = null, $indexingMemory = null)
    {
        $this->client = $client;
        $this->indexName = $indexName;
        $this->logger = $logger;
        $this->indexingMemory = $indexingMemory;
    }

    /**
     * @return \Elastica\Client
     */
    public function getClient()
    {
        return $this->client;
    }

    /**
     * @return \Elastica\Index
     */
    public function getIndex()
    {
        return $this->getClient()->getIndex($this->indexName);
    }

    /**
     * @return array|\scalar
     */
    protected function getIndexConfig()
    {
        return $this->stat('index_config');
    }

    /**
     * Performs a search query and returns either a ResultList (SS template compatible) or an Elastica\ResultSet
     * @param \Elastica\Query|string|array $query
     * @param array $options Options defined in \Elastica\Search
     * @param bool $returnResultList
     * @return ResultList
     */
    public function search($query, $options = null, $returnResultList = true)
    {
        if ($returnResultList) {
            return new ResultList($this->getIndex(), Query::create($query), $this->logger);
        }

        return $this->getIndex()->search($query, $options);
    }

    public function createIndex()
    {
        $index = $this->getIndex();

        if ($config = $this->getIndexConfig()) {
            try {
                $index->create($config, true);
            } catch (\Exception $e) {

                if ($this->logger) {
                    $this->logger->warning($e->getMessage());
                }
            }
        } else {
            $index->create();
        }
    }

    /**
     * Either creates or updates a record in the index.
     *
     * @param Searchable $record
     * @return \Elastica\Response
     */
    public function index($record)
    {
        if (!$this->indexingMemorySet && $this->indexingMemory) {

            if ($this->indexingMemory == 'unlimited') {
                increase_memory_limit_to();
            } else {
                increase_memory_limit_to($this->indexingMemory);
            }

            $this->indexingMemorySet = true;
        }

        try {

            $document = $record->getElasticaDocument();
            $type = $record->getElasticaType();
            $index = $this->getIndex();

            $response = $index->getType($type)->addDocument($document);
            $index->refresh();

            return $response;

        } catch (\Exception $e) {

            if ($this->logger) {
                $this->logger->warning($e->getMessage());
            }

        }
    }

    /**
     * @param Searchable $record
     * @return \Elastica\Response
     * @throws NotFoundException
     */
    public function remove($record)
    {
        try {

            $index = $this->getIndex();
            $type = $index->getType($record->getElasticaType());

            return $type->deleteDocument($record->getElasticaDocument());

        } catch (\Exception $e) {

            if ($this->logger) {
                $this->logger->warning($e->getMessage());
            }
        }
    }

    /**
     * Creates the index and the type mappings.
     */
    public function define()
    {
        $index = $this->getIndex();

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
     */
    public function refresh()
    {
        \Versioned::reading_stage('Live');

        foreach ($this->getIndexedClasses() as $class) {
            foreach ($class::get() as $record) {

                //Only index records with Show In Search enabled for Site Tree descendants
                //otherwise index all other data objects
                if (($record instanceof \SiteTree && $record->ShowInSearch) ||
                    (!$record instanceof \SiteTree && ($record->hasMethod('getShowInSearch') && $record->ShowInSearch)) ||
                    (!$record instanceof \SiteTree && !$record->hasMethod('getShowInSearch'))
                ) {
                    $this->index($record);
                    print "<strong>INDEXED: </strong> " . $record->getTitle() . "<br>\n";
                } else {
                    $this->remove($record);
                    print "<strong>REMOVED: </strong> " . $record->getTitle() . "<br>\n";
                }
            }
        }
    }

    /**
     * Gets the classes which are indexed (i.e. have the extension applied).
     *
     * @return array
     */
    public function getIndexedClasses()
    {
        $classes = array();

        foreach (\ClassInfo::subclassesFor('DataObject') as $candidate) {
            $candidateInstance = singleton($candidate);
            if ($candidateInstance->hasExtension('Heyday\\Elastica\\Searchable')) {
                $classes[] = $candidate;
            }
        }

        return $classes;
    }

}
