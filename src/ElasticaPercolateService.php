<?php

namespace Heyday\Elastica;


use Elastica\Client;
use Psr\Log\LoggerInterface;

class ElasticaPercolateService extends ElasticaService
{
    protected $doctypeToPercolate;

    /**
     * ElasticaPercolateService constructor.
     * @param Client $client
     * @param $indexName
     * @param LoggerInterface|null $logger
     * @param null $indexingMemory
     * @param string $searchableExtensionClassName
     * @param null $doctypeToPercolate
     */
    public function __construct(
        Client $client,
        $indexName,
        LoggerInterface $logger = null,
        $indexingMemory = null,
        $searchableExtensionClassName = Searchable::class,
        $doctypeToPercolate = null)
    {
        parent::__construct($client, $indexName, $logger, $indexingMemory, $searchableExtensionClassName);

        $this->doctypeToPercolate = $doctypeToPercolate;
    }

    /**
     * @param Searchable $record
     * @return \Elastica\Response
     */
    public function index($record)
    {
        if (!$record instanceof $this->doctypeToPercolate) {
            return parent::index($record);
        }
    }

    /**
     * @param Searchable $record
     * @return \Elastica\Response
     */
    public function remove($record)
    {
        if (!$record instanceof $this->doctypeToPercolate) {
            return parent::remove($record);
        }
    }
}