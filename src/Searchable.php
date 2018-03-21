<?php

namespace Heyday\Elastica;

use Elastica\Document;
use Elastica\Type\Mapping;
use Heyday\Elastica\Jobs\ReindexAfterWriteJob;
use Psr\Log\LoggerInterface;
use SilverStripe\Assets\File;
use SilverStripe\CMS\Model\SiteTree;
use SilverStripe\ORM\DataExtension;
use SilverStripe\ORM\DataList;
use SilverStripe\ORM\DataObject;
use SilverStripe\Versioned\Versioned;

/**
 * Adds elastic search integration to a data object.
 */
class Searchable extends DataExtension
{
    public static $published_field = 'SS_Published';

    /**
     * @config
     * @var array
     */
    public static $mappings = array(
        'PrimaryKey' => 'integer',
        'ForeignKey' => 'integer',
        'DBClassName' => 'string',
        'DBDatetime' => 'date',
        'Boolean' => 'boolean',
        'Decimal' => 'double',
        'Double' => 'double',
        'Enum' => 'string',
        'Float' => 'float',
        'HTMLText' => 'string',
        'HTMLVarchar' => 'string',
        'Int' => 'integer',
        'Datetime' => 'date',
        'Text' => 'string',
        'Varchar' => 'string',
        'Year' => 'integer',
        'File' => 'attachment',
        'Date' => 'date'
    );

    /**
     * @config
     * @var array
     */
    private static $exclude_relations = array();

    /**
     * @var ElasticaService
     */
    private $service;

    /**
     * @var bool
     */
    private $queued = false;

    /**
     * @param boolean $queued
     */
    public function setQueued($queued)
    {
        $this->queued = $queued;
    }

    /**
     * @param ElasticaService $service
     * @param LoggerInterface $logger
     */
    public function __construct(ElasticaService $service, LoggerInterface $logger = null)
    {
        $this->service = $service;
        $this->logger = $logger;
        parent::__construct();
    }

    /**
     * Returns an array of fields to be indexed. Additional configuration can be attached to these fields.
     *
     * Format: array('FieldName' => array('type' => 'string'));
     *
     * FieldName can be a field in the database or a method name
     *
     * @return array|\scalar
     */
    public function indexedFields()
    {
        return $this->owner->config()->get('indexed_fields');
    }

    /**
     * Return an array of dependant class names. These are classes that need to be reindexed when an instance of the
     * extended class is updated or when a relationship to it changes.
     * @return array|\scalar
     */
    public function dependentClasses()
    {
        return $this->owner->config()->get('dependent_classes');
    }

    /**
     * @return string
     */
    public function getElasticaType()
    {
        return get_class($this->owner);
    }

    /**
     * Replacing the SS3 inheritedDatabaseFields() method
     * @return array
     */
    public function inheritedDatabaseFields()
    {
        return $this->owner::getSchema()->fieldSpecs($this->owner->getClassName());
    }

    /**
     * Gets an array of elastic field definitions.
     * This is also where we set the type of field ($spec['type']) and the analyzer for the field ($spec['analyzer']) if needed.
     * First we go through all the regular fields belonging to pages, then to the dataobjects related to those pages
     *
     * @return array
     */
    protected function getElasticaFields()
    {
        return array_merge(
            array(self::$published_field => array('type' => 'boolean')),
            $this->getSearchableFields(),
            $this->getReferenceSearchableFields()
        );
    }


    /**
     * Get the searchable fields for the owner data object
     * @return array
     */
    protected function getSearchableFields()
    {
        $result = array();

        $fields = array_merge($this->owner->inheritedDatabaseFields(), $this->owner->config()->get('fixed_fields'));

        foreach ($this->owner->indexedFields() as $key => $fieldName) {

            if (is_array($fieldName)) { // check if data type is manually set

                $result[key($fieldName)] = $fieldName[key($fieldName)];

            } else {

                if (array_key_exists($fieldName, $fields)) { // otherwise get it from $db

                    $dataType = $this->stripDataTypeParameters($fields[$fieldName]);

                    if (array_key_exists($dataType, self::$mappings)) {
                        $spec['type'] = self::$mappings[$dataType];

                        $result[$fieldName] = array('type' => self::$mappings[$dataType]);
                    }
                }
            }
        }

        return $result;
    }

    /**
     * Get the searchable fields for the relationships of the owner data object
     * Note we currently only go one layer down eg the property of the document can be Relation_RelationField
     * @return array
     */
    protected function getReferenceSearchableFields()
    {
        $result = array();
        $config = $this->owner->config();
        $relations = array_merge($config->get('has_one'), $config->get('has_many'), $config->get('many_many'));

        foreach ($this->owner->indexedFields() as $fieldName => $params) {

            if (is_array($params)) { //If the parameters are an array, there's custom configuration
                $fieldName = key($params);
            } else {
                $fieldName = $params;
            }

            if (array_key_exists($fieldName, $relations)) { // we have an indexed field that's a relationship.
                $className = $relations[$fieldName];
                $related = singleton($className);
                $fields = $related::getSchema()->fieldSpecs($related);

                if ($related->hasExtension($this->service->searchableExtensionClassName)) {

                    foreach ($related->indexedFields() as $relatedFieldName => $relatedParams) {

                        if (is_array($relatedParams)) { //If the parameters are an array, there's custom configuration
                            $relatedFieldName = key($relatedParams);
                            $relatedParams = array_shift($relatedParams);
                        } else {
                            $relatedFieldName = $relatedParams;
                        }

                        $concatenatedFieldName = "{$fieldName}_{$relatedFieldName}"; // eg. Book_Title

                        if (isset($params[$relatedFieldName]['type'])) { //check if the data type is manually set

                            $result[$concatenatedFieldName] = $params[$relatedFieldName];

                        } else if (isset($relatedParams[$relatedFieldName]['type'])) {

                            $result[$concatenatedFieldName] = $relatedParams;

                        } else if (isset($relatedParams['type'])) {

                            $result[$concatenatedFieldName] = $relatedParams;

                        } else if (array_key_exists($relatedFieldName, $fields)) { //if not get the type from $db

                            $dataType = $this->stripDataTypeParameters($fields[$relatedFieldName]);

                            if (array_key_exists($dataType, self::$mappings)) {
                                $spec['type'] = self::$mappings[$dataType];

                                $result[$concatenatedFieldName] = array('type' => self::$mappings[$dataType]);
                            }
                        }
                    }
                }
            }
        }

        return $result;
    }

    /**
     * Clean up the data type name
     * @param $dataType
     * @return string
     */
    protected function stripDataTypeParameters($dataType)
    {
        if (($pos = strpos($dataType, '('))) {
            $dataType = substr($dataType, 0, $pos);
        }

        return $dataType;
    }

    /**
     * @param $dateString
     * @return bool|string
     */
    protected function formatDate($dateString)
    {
        return date('Y-m-d\TH:i:s', strtotime($dateString));
    }

    /**
     * @return bool|\Elastica\Type\Mapping
     */
    public function getElasticaMapping()
    {
        //Only get the mapping for non supporting types.
        if (!$this->owner->config()->get('supporting_type')) {
            $fields = $this->getElasticaFields();

            if (count($fields)) {
                $mapping = new Mapping();
                $mapping->setProperties($fields);

                return $mapping;
            }
        }

        return false;
    }

    /**
     * @param Document $document
     */
    protected function setPublishedStatus($document)
    {
        $isLive = true;
        if ($this->owner->hasExtension(Versioned::class)) {
            if ($this->owner instanceof SiteTree) {
                $isLive = $this->owner->isPublished();
            }
        }

        $document->set(self::$published_field, (bool)$isLive);
    }

    /**
     * Assigns value to the fields indexed from getElasticaFields()
     *
     * @return Document
     */
    public function getElasticaDocument()
    {
        $ownerConfig = $this->owner->config();
        $document = new Document($this->owner->ID);

        $this->setPublishedStatus($document);

        $possibleFields = array_merge(
            $this->owner->inheritedDatabaseFields(),
            $ownerConfig->get('fixed_fields')
        );

        foreach ($this->getElasticaFields() as $field => $config) {

            if (array_key_exists($field, $possibleFields) ||
                $this->owner->hasMethod('get' . $field)
            ) {

                $this->setValue($config, $field, $document, $this->owner->$field);

            } else if (strpos($field, '_') > 0) {
                $possibleRelations = array_merge(
                    $ownerConfig->get('has_one'),
                    $ownerConfig->get('has_many'),
                    $ownerConfig->get('many_many')
                );

                list($relation, $fieldName) = explode('_', $field);

                if (array_key_exists($relation, $possibleRelations)) {

                    $related = $this->owner->$relation();

                    if ($related instanceof DataObject && $related->exists()) {

                        $possibleFields = $related::getSchema()->fieldSpecs($related);

                        if (array_key_exists($fieldName, $possibleFields)) {

                            $this->setValue($config, $field, $document, $related->$fieldName);

                        } else if ($config['type'] == 'attachment') {

                            $file = $related->$fieldName();

                            if ($file instanceof File && $file->exists()) {
                                $document->addFile($field, $file->getFullPath());
                            }
                        }

                    } else if ($related instanceof DataList && $related->count()) {

                        $relatedData = [];

                        foreach ($related as $relatedItem) {
                            $data = null;

                            $possibleFields = $relatedItem::getSchema()->fieldSpecs($relatedItem);

                            if (array_key_exists($fieldName, $possibleFields) ||
                                $relatedItem->hasMethod('get' . $fieldName)
                            ) {
                                switch ($config['type']) {
                                    case 'date':
                                        if ($relatedItem->$fieldName) {
                                            $data = $this->formatDate($relatedItem->$fieldName);
                                        }
                                        break;
                                    default:
                                        $data = $relatedItem->$fieldName;
                                        break;
                                }

                            } else if ($config['type'] == 'attachment') {
                                if ($relatedItem->hasMethod('get' . $fieldName)) {
                                    $data = $relatedItem->$fieldName;
                                } else {
                                    $file = $relatedItem->$fieldName();

                                    if ($file instanceof File && $file->exists()) {
                                        $data = base64_encode(file_get_contents($file->getFullPath()));
                                    }

                                }
                            }

                            if (!is_null($data)) {
                                $relatedData[] = $data;
                            }
                        }

                        if (count($relatedData)) {
                            $document->set($field, $relatedData);
                        }
                    }
                }
            }
        }

        return $document;
    }

    /**
     * Updates the record in the search index, or removes it as necessary.
     */
    public function onAfterWrite()
    {
        if ($this->queued) {
            $reindex = new ReindexAfterWriteJob($this->owner->ID, $this->owner->ClassName);
            singleton('Symbiote\QueuedJobs\Services\QueuedJobService')->queueJob($reindex);
        } else {
            $this->reIndex();
        }
    }

    /**
     * reIndex related content
     */
    public function reIndex($stage = 'Live')
    {
        $versionToIndex = $this->owner;
        
        $currentStage = Versioned::get_stage();
        if ($stage != $currentStage) {
            $versionToIndex = Versioned::get_by_stage($this->owner->ClassName, $stage)->byID($this->owner->ID);
        }

        if (is_null($versionToIndex)) {
            return;
        }

        if (($versionToIndex instanceof SiteTree && $versionToIndex->ShowInSearch) ||
            (!$versionToIndex instanceof SiteTree && ($versionToIndex->hasMethod('getShowInSearch') && $versionToIndex->ShowInSearch)) ||
            (!$versionToIndex instanceof SiteTree && !$versionToIndex->hasMethod('getShowInSearch'))
        ) {
            $this->service->index($versionToIndex);
        } else {
            $this->service->remove($versionToIndex);
        }

        $this->updateDependentClasses();

    }

    /**
     * Removes the record from the search index.
     */
    public function onBeforeDelete()
    {
        $this->service->remove($this->owner);
        if ($this->queued) {
            $reindex = new ReindexAfterWriteJob($this->owner);
            singleton('QueuedJobService')->queueJob($reindex);
        } else {
            $this->updateDependentClasses();
        }
    }

    /**
     * Update dependent classes after the extended object has been removed from a ManyManyList
     */
    public function onAfterManyManyRelationRemove()
    {
        if ($this->queued) {
            $reindex = new ReindexAfterWriteJob($this->owner);
            singleton('QueuedJobService')->queueJob($reindex);
        } else {
            $this->updateDependentClasses();
        }
    }

    /**
     * Update dependent classes after the extended object has been added to a ManyManyList
     */
    public function onAfterManyManyRelationAdd()
    {
        if ($this->queued) {
            $reindex = new ReindexAfterWriteJob($this->owner);
            singleton('QueuedJobService')->queueJob($reindex);
        } else {
            $this->updateDependentClasses();
        }
    }

    /**
     * Updates the records of all instances of dependent classes.
     */
    protected function updateDependentClasses()
    {
        $classes = $this->dependentClasses();
        if ($classes) {
            foreach ($classes as $class) {
                $list = DataList::create($class);

                foreach ($list as $object) {

                    if ($object instanceof DataObject &&
                        $object->hasExtension($this->service->searchableExtensionClassName)
                    ) {
                        if (($object instanceof SiteTree && $object->ShowInSearch) ||
                            (!$object instanceof SiteTree)
                        ) {
                            $this->service->index($object);
                        } else {
                            $this->service->remove($object);
                        }
                    }
                }
            }
        }
    }

    /**
     * @param $config
     * @param $fieldName
     * @param \Elastica\Document $document
     * @param $fieldValue
     */
    public function setValue($config, $fieldName, $document, $fieldValue)
    {
        switch ($config['type']) {
            case 'boolean':
                $document->set($fieldName, boolval($fieldValue) ? 'true' : 'false');
                break;
            case 'date':
                if ($fieldValue) {
                    $document->set($fieldName, $this->formatDate($fieldValue));
                }
                break;
            default:
                $document->set($fieldName, $fieldValue);
                break;
        }
    }

}
