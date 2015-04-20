<?php

namespace Heyday\Elastica;

use Elastica\Client;
use Elastica\Exception\NotFoundException;
use Elastica\Query;
use Psr\Log\LoggerInterface;

/**
 * A service used to interact with elastic search.
 */
class ElasticaService
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
     * @param Client $client
     * @param string $indexName
     * @param LoggerInterface $logger
     */
    public function __construct(Client $client, $indexName, LoggerInterface $logger = null)
    {
        $this->client = $client;
        $this->indexName = $indexName;
        $this->logger = $logger;
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
     * Performs a search query and returns a result list.
     *
     * @param \Elastica\Query|string|array $query
     * @return ResultList
     */
    public function search($query)
    {
        return new ResultList($this->getIndex(), Query::create($query), $this->logger);
    }

    /**
     * Either creates or updates a record in the index.
     *
     * @param Searchable $record
     * @return \Elastica\Response
     */
    public function index($record)
    {
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
            $index->create();
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
        foreach ($this->getIndexedClasses() as $class) {
            foreach ($class::get() as $record) {

                //Only index records with Show In Search enabled for Site Tree descendants
                //otherwise index all other data objects
                if (($record instanceof \SiteTree && $record->ShowInSearch) ||
                    (!$record instanceof \SiteTree && $record instanceof \DataObject)
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
            if ($candidateInstance->hasExtension('Heyday\\Elastica\\SilverStripe\\Searchable')) {
                $classes[] = $candidate;
            }
        }

        return $classes;
    }

}
