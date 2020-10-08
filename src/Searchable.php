<?php

namespace Heyday\Elastica;

use BadMethodCallException;
use Elastica\Document;
use Elastica\Mapping;
use Exception;
use Heyday\Elastica\Jobs\ReindexAfterWriteJob;
use Psr\Log\LoggerInterface;
use SilverStripe\Assets\File;
use SilverStripe\CMS\Model\SiteTree;
use SilverStripe\Dev\Deprecation;
use SilverStripe\ORM\DataExtension;
use SilverStripe\ORM\DataList;
use SilverStripe\ORM\DataObject;
use SilverStripe\ORM\DataObjectSchema;
use SilverStripe\Versioned\Versioned;
use Symbiote\QueuedJobs\Services\QueuedJobService;
use function is_numeric;

/**
 * Adds elastic search integration to a data object.
 *
 * @property DataObject|Searchable $owner
 */
class Searchable extends DataExtension
{
    public static $published_field = 'SS_Published';

    /**
     * @config
     * @var array
     */
    private static $elasticsearch_field_mappings = [
        'PrimaryKey'  => 'integer',
        'ForeignKey'  => 'integer',
        'DBClassName' => 'keyword',
        'DBDatetime'  => 'date',
        'Boolean'     => 'boolean',
        'Decimal'     => 'double',
        'Double'      => 'double',
        'Enum'        => 'keyword',
        'Float'       => 'float',
        'HTMLText'    => 'text',
        'HTMLVarchar' => 'text',
        'Int'         => 'integer',
        'Datetime'    => 'date',
        'Text'        => 'text',
        'Varchar'     => 'text',
        'Year'        => 'integer',
        'File'        => 'attachment',
        'Date'        => 'date'
    ];

    /**
     * ElasticSearch 7.0 compatibility: Use a custom 'type' field instead of deprecated _type
     * @link https://www.elastic.co/guide/en/elasticsearch/reference/current/removal-of-types.html#_custom_type_field
     *
     * @var array
     * @config
     */
    private static $indexed_fields = [
        'type' => ['type' => 'keyword'],
    ];

    /**
     * @config
     * @var array
     */
    private static $exclude_relations = [];

    /**
     * @var ElasticaService
     */
    private $service;

    /**
     * @var LoggerInterface
     */
    protected $logger = null;

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
     * Check if queued jobs for reindexing is enabled
     *
     * @return bool
     */
    protected function getUseQueuedJobs()
    {
        return $this->queued && class_exists(QueuedJobService::class);
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
     * Format: ['FieldName' => ['type' => 'text']];
     *
     * FieldName can be a field in the database or a method name
     *
     * @return array
     */
    public function indexedFields()
    {
        $fields = $this->owner->config()->get('indexed_fields');
        $normalised = [];
        foreach ($fields as $fieldName => $params) {
            // Normalise field into name, specs (array)
            if (is_array($params) && is_numeric($fieldName)) {
                // Field name => specs are nested
                $fieldName = key($params);
                $params = array_shift($params);
            } elseif (is_numeric($fieldName)) {
                // Field name only is specified as value
                $fieldName = $params;
                $params = [];
            }
            $normalised[$fieldName] = $params;
        }
        return $normalised;
    }

    /**
     * Return an array of dependant class names. These are classes that need to be reindexed when an instance of the
     * extended class is updated or when a relationship to it changes.
     * @return array
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
        return $this->owner->getSchema()->fieldSpecs($this->owner->getClassName());
    }

    /**
     * Gets an array of elastic field definitions.
     * This is also where we set the type of field ($spec['type']) and the analyzer for the field ($spec['analyzer'])
     * if needed. First we go through all the regular fields belonging to pages, then to the dataobjects related to
     * those pages
     *
     * @return array
     */
    protected function getElasticaFields()
    {
        return array_merge(
            [
                self::$published_field => [
                    'type' => 'boolean'
                ]
            ],
            $this->getSearchableFields()
        );
    }


    /**
     * Get the searchable fields for the owner data object
     *
     * @return array
     */
    public function getSearchableFields()
    {
        $result = [];
        foreach ($this->owner->indexedFields() as $fieldName => $params) {
            // Check nested relation class
            $relationClass = isset($params['relationClass'])
                ? $params['relationClass']
                : $this->owner->getRelationClass($fieldName);
            unset($params['relationClass']); // Don't send to elasticsearch

            // Build nested field from relation
            if ($relationClass) {
                // Relations can add multiple fields, so merge them all here
                $nestedFields = $this->getSearchableFieldsForRelation($fieldName, $params, $relationClass);
                $result = array_merge($result, $nestedFields);
                continue;
            }

            // Get extra params
            $params = $this->getExtraFieldParams($fieldName, $params);

            // Add field
            $result[$fieldName] = $params;
        }

        return $result;
    }

    /**
     * Get the searchable fields for the relationships of the owner data object
     * Note we currently only go one layer down eg the property of the document can be Relation_RelationField
     *
     * @return array
     * @deprecated
     */
    protected function getReferenceSearchableFields()
    {
        Deprecation::notice('2.0.0', 'Use getSearchableFields instead');
        return $this->getSearchableFields();
    }

    /**
     * Clean up the data type name
     * @param string $dataType
     * @return string
     */
    protected function stripDataTypeParameters($dataType)
    {
        return strtok($dataType, '(');
    }

    /**
     * @param string $dateString
     * @return string|null
     */
    protected function formatDate($dateString)
    {
        if (empty($dateString)) {
            return null;
        }
        return date('Y-m-d\TH:i:s', strtotime($dateString));
    }

    /**
     * Coerce strings into integers
     *
     * @param mixed $intString
     * @return int|null
     */
    protected function formatInt($intString)
    {
        if (is_null($intString)) {
            return null;
        }
        return (int)$intString;
    }

    /**
     * Coerce strings into floats
     *
     * @param mixed $floatString
     * @return float|null
     */
    protected function formatFloat($floatString)
    {
        if (is_null($floatString)) {
            return null;
        }
        return (float)$floatString;
    }

    /**
     * @return bool|Mapping
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
        $document = new Document($this->owner->ID);

        // Set published state
        $this->setPublishedStatus($document);

        // Add all nested field values
        foreach ($this->getSearchableFieldValues() as $field => $value) {
            $document->set($field, $value);
        }

        return $document;
    }

    /**
     * Get values for all searchable fields as an array.
     * Similr to getSearchableFields() but returns field values instead of spec
     *
     * @return array
     */
    public function getSearchableFieldValues()
    {
        $fieldValues = [];
        foreach ($this->owner->indexedFields() as $fieldName => $params) {
            // Check nested relation class
            $relationClass = isset($params['relationClass'])
                ? $params['relationClass']
                : $this->owner->getRelationClass($fieldName);
            unset($params['relationClass']); // Don't send to elasticsearch

            // Build nested field from relation
            if ($relationClass) {
                // Relations can add multiple fields, so merge them all here
                $nestedFieldValues = $this->getSearchableFieldValuesForRelation($fieldName, $params, $relationClass);
                $fieldValues = array_merge($fieldValues, $nestedFieldValues);
                continue;
            }

            if ($fieldName === 'type') {
                // Check 'type' field
                $fieldValues['type'] = $this->getElasticaType();
            } elseif ($this->owner->hasField($fieldName)) {
                // Check field exists on parent
                $params = $this->getExtraFieldParams($fieldName, $params);
                $fieldValue = $this->formatValue($params, $this->owner->$fieldName);
                $fieldValues[$fieldName] = $fieldValue;
            }
        }

        return $fieldValues;
    }

    /**
     * Updates the record in the search index, or removes it as necessary.
     * @throws Exception
     */
    public function onAfterWrite()
    {
        if ($this->getUseQueuedJobs()) {
            $this->queueReindex();
        } else {
            $this->reIndex();
        }
    }

    /**
     * reIndex related content
     *
     * @param string $stage
     * @throws Exception
     */
    public function reIndex($stage = Versioned::LIVE)
    {
        $versionToIndex = $this->owner;

        $currentStage = Versioned::get_stage();
        if ($stage !== $currentStage) {
            $versionToIndex = Versioned::get_by_stage($this->owner->ClassName, $stage)->byID($this->owner->ID);
        }

        if (is_null($versionToIndex)) {
            return;
        }

        if (!$versionToIndex->hasField('ShowInSearch') || $versionToIndex->ShowInSearch) {
            $this->service->index($versionToIndex);
        } else {
            $this->service->remove($versionToIndex);
        }

        $this->updateDependentClasses();
    }

    /**
     * Batch update all documents attached to the index for this record
     *
     * @param callable $callback
     * @param int      $documentsProcessed
     * @return mixed
     * @throws Exception
     */
    public function batchIndex(callable $callback, &$documentsProcessed = 0)
    {
        return $this->service->batch($callback, $documentsProcessed);
    }

    /**
     * Removes the record from the search index.
     * @throws Exception
     */
    public function onBeforeDelete()
    {
        $this->service->remove($this->owner);
        if ($this->getUseQueuedJobs()) {
            $this->queueReindex();
        } else {
            $this->updateDependentClasses();
        }
    }

    /**
     * Update dependent classes after the extended object has been removed from a ManyManyList
     * @throws Exception
     */
    public function onAfterManyManyRelationRemove()
    {
        if ($this->getUseQueuedJobs()) {
            $this->queueReindex();
        } else {
            $this->updateDependentClasses();
        }
    }

    /**
     * Update dependent classes after the extended object has been added to a ManyManyList
     * @throws Exception
     */
    public function onAfterManyManyRelationAdd()
    {
        if ($this->getUseQueuedJobs()) {
            $this->queueReindex();
        } else {
            $this->updateDependentClasses();
        }
    }

    /**
     * Updates the records of all instances of dependent classes.
     * @throws Exception
     */
    protected function updateDependentClasses()
    {
        $classes = $this->dependentClasses();
        if ($classes) {
            foreach ($classes as $class) {
                $list = DataList::create($class);

                foreach ($list as $object) {
                    if ($object instanceof DataObject && $object->hasExtension(Searchable::class)) {
                        if (!$object->hasField('ShowInSearch') || $object->ShowInSearch) {
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
     * Serialise a file attachment
     *
     * @param File $file
     * @return array Value for 'attachment' type
     */
    protected function createAttachment(File $file)
    {
        $value = base64_encode($file->getStream());
        $mimeType = $file->getMimeType();

        return [
            '_content_type' => $mimeType,
            '_name'         => $file->Name,
            '_content'      => $value,
        ];
    }

    /**
     * Build searchable spec for a given field
     *
     * @param string $fieldName
     * @param array  $params Spec params
     * @param string $className
     * @return array
     */
    protected function getSearchableFieldsForRelation($fieldName, $params, $className)
    {
        // Detect attachment; Skip relational check
        if (isset($params['type']) && $params['type'] === 'attachment') {
            return [$fieldName => $params];
        }

        // Skip if this relation class has no elasticsearch content
        /** @var DataObject|Searchable $related */
        $related = DataObject::singleton($className);
        if (!$related->hasExtension(Searchable::class)) {
            return [];
        }

        // Get nested fields
        $nestedFields = $related->getSearchableFields();

        // Determine if merging into parent as either a multilevel object (default)
        // or nested objects (requires 'nested' param to be set)
        if (isset($params['type']) && $params['type'] === 'nested') {
            // Set nested fields
            // https://www.elastic.co/guide/en/elasticsearch/guide/current/nested-mapping.html
            // https://www.elastic.co/guide/en/elasticsearch/reference/5.6/nested.html
            return [
                $fieldName => array_merge(
                    $params,
                    ['properties' => $nestedFields]
                )
            ];
        }

        // If not nested default to multilevel object
        $newFields = [];
        foreach ($nestedFields as $relatedFieldName => $relatedParams) {
            // Flatten each field as a sub_name. E.g. Book_Title
            $nestedName = "{$fieldName}_{$relatedFieldName}";
            $newFields[$nestedName] = $relatedParams;
        }
        return $newFields;
    }

    /**
     * Get all fields from a relation on a parent object
     *
     * @param string $fieldName
     * @param array  $params Spec params
     * @param string $className
     * @return array
     */
    protected function getSearchableFieldValuesForRelation($fieldName, $params, $className)
    {
        // Detect attachment
        if (isset($params['type']) && $params['type'] === 'attachment') {
            /** @var File $file */
            $file = $this->owner->$fieldName();
            if (!$file instanceof File || !$file->exists()) {
                return [];
            }
            return [$fieldName => $this->createAttachment($file)];
        }

        // Skip if this relation class has no elasticsearch content
        /** @var DataObject|Searchable $relatedSingleton */
        $relatedSingleton = DataObject::singleton($className);
        if (!$relatedSingleton->hasExtension(Searchable::class)) {
            return [];
        }

        // Get item from parent
        $relatedList = $this->owner->$fieldName();
        if (!$relatedList) {
            return [];
        }

        // Handle unary relations
        /** @var DataObject|Searchable $relatedItem */
        $relatedItem = null;
        // Handle unary sets
        $isUnary = $relatedList instanceof DataObject;
        if ($isUnary) {
            $relatedItem = $relatedList;
            $relatedList = [$relatedItem];
        }

        // Determine if merging into parent as either a multilevel object (default)
        // or nested objects (requires 'nested' param to be set)
        // Note: Unary relations are treated as a single-length list
        if (isset($params['type']) && $params['type'] === 'nested') {
            $relationValues = [];
            /** @var DataObject|Searchable $relationListItem */
            foreach ($relatedList as $relationListItem) {
                $relationValues[] = $relationListItem->getSearchableFieldValues();
            }
            return [$fieldName => $relationValues];
        }

        // If not nested default to multilevel object

        // Handle unary-multilevel
        // I.e. Relation_Field = 'value'
        if ($isUnary) {
            // We will return multiple values, one for each sub-column
            $fieldValues = [];
            foreach ($relatedItem->getSearchableFieldValues() as $relatedFieldName => $relatedFieldValue) {
                $nestedName = "{$fieldName}_{$relatedFieldName}";
                $fieldValues[$nestedName] = $relatedItem->IsInDB() ? $relatedFieldValue : null;
            }
            return $fieldValues;
        }

        // Handle non-unary-multilevel
        // I.e. Relation_Field = ['value1', 'value2']
        $fieldValues = [];

        // Bootstrap set with empty arrays for each top level key
        // This also ensures we set empty data if $relatedList is empty
        foreach ($relatedSingleton->getSearchableFields() as $relatedFieldName => $spec) {
            $nestedName = "{$fieldName}_{$relatedFieldName}";
            $fieldValues[$nestedName] = [];
        }

        // Add all documents to the list
        foreach ($relatedList as $relatedListItem) {
            foreach ($relatedListItem->getSearchableFieldValues() as $relatedFieldName => $relatedFieldValue) {
                $nestedName = "{$fieldName}_{$relatedFieldName}";
                $fieldValues[$nestedName][] = $relatedFieldValue;
            }
        }
        return $fieldValues;
    }

    /**
     * @param $fieldValue
     * @return string
     */
    protected function formatBoolean($fieldValue)
    {
        return boolval($fieldValue) ? 'true' : 'false';
    }

    /**
     * Format a scalar value for the index document
     *
     * @param array $params Spec params
     * @param mixed $fieldValue
     * @return mixed
     */
    protected function formatValue($params, $fieldValue)
    {
        $type = isset($params['type']) ? $params['type'] : null;
        switch ($type) {
            case 'boolean':
                return $this->formatBoolean($fieldValue);
            case 'date':
                return $this->formatDate($fieldValue);
            case 'integer':
                return $this->formatInt($fieldValue);
            case 'float':
                return $this->formatFloat($fieldValue);
            default:
                return $fieldValue;
        }
    }

    /**
     * Get extra params for a field from the parent document
     *
     * @param string $fieldName
     * @param array  $params
     * @return array
     */
    protected function getExtraFieldParams($fieldName, $params)
    {
        // Skip if type is already define
        if (isset($params['type'])) {
            return $params;
        }

        // Guess type from $db spec
        $fieldType = DataObjectSchema::singleton()->fieldSpec($this->owner, $fieldName);
        if ($fieldType) {
            // Strip and check data type mapping
            $dataType = $this->stripDataTypeParameters($fieldType);
            $mappings = $this->owner->config()->get('elasticsearch_field_mappings');
            if (array_key_exists($dataType, $mappings)) {
                $params['type'] = $mappings[$dataType];
            }
        }
        return $params;
    }

    /**
     * Trigger a queuedjob to update this item.
     * Require queuedjobs to be setup.
     */
    protected function queueReindex()
    {
        if (!$this->getUseQueuedJobs()) {
            throw new BadMethodCallException("Queued is disabled or queuedjobs module is not installed");
        }

        $reindex = new ReindexAfterWriteJob($this->owner->ID, $this->owner->ClassName);
        QueuedJobService::singleton()->queueJob($reindex);
    }
}
