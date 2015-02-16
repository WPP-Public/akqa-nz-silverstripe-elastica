<?php

namespace SilverStripe\Elastica;

use Elastica\Client;
use Elastica\Exception\NotFoundException;
use Elastica\Query;

/**
 * A service used to interact with elastic search.
 */
class ElasticaService
{

    private $client;
    private $index;

    /**
     * @param \Elastica\Client $client
     * @param string $index
     */
    public function __construct(Client $client, $index, Logger $logger = null)
    {
        $this->client = $client;
        $this->index = $index;
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
        return $this->getClient()->getIndex($this->index);
    }

    /**
     * Performs a search query and returns a result list.
     *
     * @param \Elastica\Query|string|array $query
     * @return ResultList
     */
    public function search($query)
    {
        return new ResultList($this->getIndex(), Query::create($query));
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
                $this->logger->log($e->getMessage());
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
                $this->logger->log($e->getMessage());
            }

            if ($e instanceof NotFoundException) {
                throw $e;
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
            $mapping->setType($index->getType($sng->getElasticaType()));
            $mapping->send();
        }
    }

    /**
     * Re-indexes each record in the index.
     */
    public function refresh()
    {
        foreach ($this->getIndexedClasses() as $class) {
            foreach ($class::get() as $record) {

                if ($record->ShowInSearch) {
                    $this->index($record);
                    print "<strong>INDEXED: </strong> " . $record->getTitle() . "<br>\n";
                } else {
                    print "<strong>Attempting to remove: </strong> " . $record->getTitle() . "<br>\n";

                    try {
                        $this->remove($record)->getData();
                        print "<strong>REMOVED: </strong> " . $record->getTitle() . "<br>\n";
                    } catch (NotFoundException $e) {
                        print "<strong>NOT INDEXED: </strong> " . $record->getTitle() . "- {$e->getMessage()} <br>\n";
                    }
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
            if (singleton($candidate)->hasExtension('SilverStripe\\Elastica\\Searchable')) {
                $classes[] = $candidate;
            }
        }

        return $classes;
    }

}
