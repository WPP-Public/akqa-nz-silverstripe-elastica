<?php

namespace SilverStripe\Elastica;

use Elastica\Document;
use Elastica\Type\Mapping;
use SilverStripe\Elastica\Interfaces\ElasticSearchFieldsInterface;

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
        'File' => 'attachment'
    );

    /**
     * @config
     * @var array
     */
    private static $exclude_relations = array();

    private $service;

    public function __construct(ElasticaService $service, Logger $logger = null)
    {
        $this->service = $service;
        $this->logger = $logger;
        parent::__construct();
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
    public function getElasticaFields()
    {
        $result = $this->getSearchableFields(array());

        return $this->getReferenceSearchableFields($result);
    }

    /**
     * Get the searchable fields for the owner data object
     * @return array
     */
    protected function getSearchableFields(array $result)
    {
        $fields = $this->owner->inheritedDatabaseFields();

        foreach ($this->getUnprocessedSearchableFields($this->owner) as $fieldName => $params) {
            $type = null;
            $spec = array(
                'IsReference' => false
            );

            if (array_key_exists($fieldName, $fields)) {
                $dataType = $this->stripDataTypeParameters($fields[$fieldName]);

                if (array_key_exists($dataType, self::$mappings)) {
                    $spec['type'] = self::$mappings[$dataType];
                }
            } else { //handle File Contents
                $spec = array_merge($spec, $params);
            }

            $result[$fieldName] = $spec;
        }

        return $result;
    }


    /**
     * @param array $result
     * @return array
     */
    protected function getReferenceSearchableFields(array $result)
    {
        //now loop through DataObjects related to $this->owner and get all searchable fields of those DO
        foreach (array($this->owner->has_many(), $this->owner->has_one(), $this->owner->many_many()) as $relationship) {
            foreach ($relationship as $reference => $className) {
                if ($this->owner->$reference() instanceof \ArrayAccess && !in_array($reference, $this->getExcludedRelations())) {

                    foreach ($this->owner->$reference() as $dataObject) {
                        $fields = \DataObject::database_fields(get_class($dataObject));

                        foreach ($this->getUnprocessedSearchableFields($dataObject) as $fieldName => $params) {

                            if (array_key_exists($fieldName, $fields)) {
                                $dataType = $this->stripDataTypeParameters($fields[$fieldName]);

                                if (array_key_exists($dataType, self::$mappings)) {

                                    $result[$reference . '_' . $fieldName] = array(
                                        'IsReference' => true,
                                        'type' => self::$mappings[$dataType],
                                        'ReferenceName' => $reference,
                                        'FieldName' => $fieldName
                                    );
                                } else {
                                    $result[$reference . '_' . $fieldName] = array_merge(array(
                                        'IsReference' => true,
                                        'ReferenceName' => $reference,
                                        'FieldName' => $fieldName
                                    ), $params);
                                }
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
     * Get all the eligible searchable fields from the dataobject
     * @param \DataObject $dataObject
     * @return array
     */
    protected function getUnprocessedSearchableFields(\DataObject $dataObject)
    {
        if ($dataObject instanceof ElasticSearchFieldsInterface) {
            return $dataObject->searchableFields() + $dataObject->elasticSearchFields();
        }

        return $dataObject->searchableFields();
    }

    /**
     * @return \Elastica\Type\Mapping
     */
    public function getElasticaMapping()
    {
        $mapping = new Mapping();
        $mapping->setProperties($this->getElasticaFields());

        return $mapping;
    }

    /**
     * Assigns value to the fields indexed from getElasticaFields()
     *
     * @return Document
     */
    public function getElasticaDocument()
    {
        $fields = array();

        foreach ($this->getElasticaFields() as $field => $config) {

            // Handle Referenced DataObjects
            if (isset($config['IsReference']) && $config['IsReference']) {

                $referenceName = $config['ReferenceName'];
                $fieldName = $config['FieldName'];
                $index = $referenceName . '_' . $fieldName;

                foreach ($this->owner->$referenceName() as $dataObject) {

                    if (!isset($fields[$index])) {
                        $fields[$index] = '';
                    }

                    $fields[$index] .= ' ' . $dataObject->$fieldName;
                }
            } else {

                if ($field == 'FileContent') { //handle files
                    $fields[$field] = base64_encode(file_get_contents($this->owner->getFullPath()));
                } elseif ($field == 'LastEdited') { //handle Last_Edited field
                    //transform into valid date field according to elastica, otherwise it complains

                    if ($this->owner->$field) {
                        $date = str_replace(' ', 'T', $this->owner->$field);

                        if ($date == '0000-00-00T00:00:00') {
                            $fields[$field] = date("Y-m-d");
                        } else {
                            $fields[$field] = $date;
                        }
                    }
                } else { //handle regular fields from PageTypes
                    $fields[$field] = $this->owner->$field;
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

        if ($this->owner->ShowInSearch) {
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
