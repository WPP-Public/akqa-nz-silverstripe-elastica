<?php

namespace SilverStripe\Elastica;

use Elastica\Document;
use Elastica\Type\Mapping;
use Psr\Log\LoggerInterface;

/**
 * Adds elastic search integration to a data object.
 */
class Searchable extends \DataExtension
{

    /**
     * @config
     * @var array
     */
    public static $mappings = array(
        'Boolean' => 'boolean',
        'Decimal' => 'double',
        'Double' => 'double',
        'Enum' => 'string',
        'Float' => 'float',
        'HTMLText' => 'string',
        'HTMLVarchar' => 'string',
        'Int' => 'integer',
        'SS_Datetime' => 'date',
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

    private $service;

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
     * @return array
     */
    public function indexedFields()
    {
        return array();
    }

    public function getExcludedRelations()
    {
        return \Config::inst()->forClass(get_called_class())->excluded_relations;
    }

    /**
     * @return string
     */
    public function getElasticaType()
    {
        return get_class($this->owner);
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
        return array_merge($this->getSearchableFields(), $this->getReferenceSearchableFields());
    }

    /**
     * Get the searchable fields for the owner data object
     * @return array
     */
    protected function getSearchableFields()
    {
        $result = array();

        $fields = $this->owner->inheritedDatabaseFields();

        foreach ($this->owner->indexedFields() as $fieldName => $params) {

            if (isset($params['type'])) {

                $result[$fieldName] = $params;

            } else {

                $fieldName = $params;

                if (array_key_exists($fieldName, $fields)) {

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
     * @return array
     */
    protected function getReferenceSearchableFields()
    {
        $result = array();

        $relations = array_merge($this->owner->has_many(), $this->owner->has_one(), $this->owner->many_many());

        foreach ($this->owner->indexedFields() as $fieldName => $params) {

            if (is_int($fieldName)) {
                $fieldName = $params;
            }

            if (array_key_exists($fieldName, $relations)) {

                $className = $relations[$fieldName];
                $related = singleton($className);
                $fields = $related->inheritedDatabaseFields();

                if ($related->hasExtension('SilverStripe\\Elastica\\Searchable')) {

                    foreach ($related->indexedFields() as $relatedFieldName => $relatedParams) {

                        if (is_int($relatedFieldName)) {
                            $relatedFieldName = $relatedParams;
                        }

                        $concatenatedFieldName = "{$fieldName}_{$relatedFieldName}";

                        if (isset($params[$relatedFieldName]['type'])) {

                            $result[$concatenatedFieldName] = $params[$relatedFieldName];

                        } else if (isset($relatedParams[$relatedFieldName]['type'])) {

                            $result[$concatenatedFieldName] = $relatedParams;

                        } else if (array_key_exists($relatedFieldName, $fields)) {

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

    protected function stripDataTypeParameters($dataType)
    {
        if (($pos = strpos($dataType, '('))) {
            $dataType = substr($dataType, 0, $pos);
        }

        return $dataType;
    }

    /**
     * @return bool|\Elastica\Type\Mapping
     */
    public function getElasticaMapping()
    {
        $fields = $this->getElasticaFields();

        if (count($fields)) {
            $mapping = new Mapping();
            $mapping->setProperties($this->getElasticaFields());

            return $mapping;
        }

        return false;
    }

    /**
     * Assigns value to the fields indexed from getElasticaFields()
     *
     * @return Document
     */
    public function getElasticaDocument()
    {
        $possibleFields = $this->owner->inheritedDatabaseFields();

        $fields = array();

        foreach ($this->getElasticaFields() as $field => $config) {


            if (array_key_exists($field, $possibleFields) ||
                $this->owner->hasMethod('get' . $field)
            ) {

                switch ($config['type']) {
                    case 'date':

                        $date = str_replace(' ', 'T', $this->owner->$field);

                        if ($date != '0000-00-00T00:00:00') {
                            $fields[$field] = $date;
                        }
                        break;
                    default:
                        $fields[$field] = $this->owner->$field;
                        break;
                }

            } else {

                $possibleRelations = array_merge($this->owner->has_many(), $this->owner->has_one(), $this->owner->many_many());

                list($relation, $fieldName) = explode('_', $field);

                if (array_key_exists($relation, $possibleRelations)) {

                    $related = $this->owner->$relation();

                    if ($related instanceof \DataObject && $related->exists()) {

                        $possibleFields = $related->inheritedDatabaseFields();

                        if (array_key_exists($fieldName, $possibleFields)) {
                            $fields[$field] = $related->$fieldName;
                        }


                    } else if ($related instanceof \DataList && $related->count()) {

                        $relatedData = [];

                        foreach ($related as $relatedItem) {

                            $possibleFields = $relatedItem->inheritedDatabaseFields();

                            if (array_key_exists($fieldName, $possibleFields) ||
                                $related->hasMethod('get' . $fieldName)
                            ) {
                                $data = $relatedItem->$fieldName;

                                if (!is_null($data)) {
                                    $relatedData[] = $data;
                                }
                            }
                        }

                        if (count($relatedData)) {
                            $fields[$field] = $relatedData;
                        }
                    }
                }
            }

        }

        return new Document($this->owner->ID, $fields);
    }

    /**
     * Updates the record in the search index, or removes it as necessary.
     */
    public function onAfterWrite()
    {

        if (($this->owner instanceof \SiteTree && $this->owner->ShowInSearch)
            || (!$this->owner instanceof \SiteTree && $this->owner instanceof \DataObject)
        ) {
            $this->service->index($this->owner);
        } else {
            $this->service->remove($this->owner);
        }
    }

    /**
     * Removes the record from the search index.
     */
    public function onAfterDelete()
    {
        $this->service->remove($this->owner);
    }

}
