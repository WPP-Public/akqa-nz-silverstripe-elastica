<?php

namespace Heyday\Elastica;

use Elastica\Client;
use Elastica\Exception\NotFoundException;
use Elastica\Query;
use Psr\Log\LoggerInterface;
use SilverStripe\CMS\Model\SiteTree;
use SilverStripe\Control\Director;
use SilverStripe\Core\ClassInfo;
use SilverStripe\Core\Config\Config;
use SilverStripe\Core\Config\Configurable;
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
     * ElasticaService constructor.
     * @param Client $client
     * @param $indexName
     * @param LoggerInterface|null $logger Increases the memory limit while indexing. A memory limit string, such as "64M".
     * @param null $indexingMemory
     * @param string $searchableExtensionClassName
     */
    public function __construct(
        Client $client,
        $indexName,
        LoggerInterface $logger = null,
        $indexingMemory = null,
        $searchableExtensionClassName = Searchable::class
    )
    {
        $this->client = $client;
        $this->indexName = $indexName;
        $this->logger = $logger;
        $this->indexingMemory = $indexingMemory;
        $this->searchableExtensionClassName = $searchableExtensionClassName;
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
        return $this->config()->get('index_config');
    }

    /**
     * Performs a search query and returns either a ResultList (SS template compatible) or an Elastica\ResultSet
     * @param \Elastica\Query|string|array $query
     * @param array $options Options defined in \Elastica\Search
     * @param bool $returnResultList
     * @return ResultList | \Elastica\ResultSet
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
        if (!$this->config()->get(self::CONFIGURE_DISABLE_INDEXING)
            && !$record->config()->get('supporting_type')) {
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
    }

    /**
     * @param Searchable $record
     * @return \Elastica\Response
     * @throws NotFoundException
     */
    public function remove($record)
    {
        if (!$this->config()->get(self::CONFIGURE_DISABLE_INDEXING)
            && !$record->config()->get('supporting_type')) {
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
        $reading_mode = Versioned::get_reading_mode();
        Versioned::set_reading_mode('Stage.Live');

        foreach ($this->getIndexedClasses() as $class) {
            if (!Config::inst()->get($class, 'supporting_type')) { //Only index types (or classes) that are not just supporting other index types
                foreach ($class::get() as $record) {

                    //Only index records with Show In Search enabled for Site Tree descendants
                    //otherwise index all other data objects
                    if (($record instanceof SiteTree && $record->ShowInSearch) ||
                        (!$record instanceof SiteTree && ($record->hasMethod('getShowInSearch') && $record->getShowInSearch())) ||
                        (!$record instanceof SiteTree && !$record->hasMethod('getShowInSearch'))
                    ) {
                        $this->index($record);
                        if (Director::is_cli()) {
                            print "INDEXED: Document Type \"" . $record->getClassName() . "\" - " . $record->getTitle() . " - ID " . $record->ID . "\n";
                        } else {
                            print "<strong>INDEXED: </strong>Document Type \"" . $record->getClassName() . "\" - " . $record->getTitle() . " - ID " . $record->ID . "<br>";
                        }

                    } else {
                        $this->remove($record);
                        if (Director::is_cli()) {
                            print "REMOVED: Document Type \"" . $record->getClassName() . "\" - " . $record->getTitle() . " - ID " . $record->ID . "\n";
                        } else {
                            print "<strong>REMOVED: </strong>Document Type \"" . $record->getClassName() . "\" - " . $record->getTitle() . " - ID " . $record->ID . "<br>";
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
        foreach (ClassInfo::subclassesFor('SilverStripe\ORM\DataObject') as $candidate) {
            $candidateInstance = singleton($candidate);
            if ($candidateInstance->hasExtension($this->searchableExtensionClassName) && $candidate != 'Page') {
                $classes[] = $candidate;
            }
        }
        return $classes;
    }

}
