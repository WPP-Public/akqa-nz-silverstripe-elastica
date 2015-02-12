<?php

namespace SilverStripe\Elastica;

use Elastica\Document;
use Elastica\Exception\NotFoundException;
use Elastica\Type\Mapping;

/**
 * Adds elastic search integration to a data object.
 */
class Searchable extends \DataExtension
{

    public static $mappings = array(
        'Boolean' => 'boolean',
        'Decimal' => 'double',
        'Double' => 'double',
        'Enum' => 'string',
        'Float' => 'float',
        'HTMLText' => 'string',
        'Varchar(255)' => 'string',
        'Varchar(50)' => 'string',
        'HTMLVarchar' => 'string',
        'Int' => 'integer',
        'SS_Datetime' => 'date',
        'Text' => 'string',
        'Varchar' => 'string',
        'Year' => 'integer',
        'File' => 'attachment'
    );

    private $service;

    public function __construct(ElasticaService $service, Logger $logger = null)
    {
        $this->service = $service;
        $this->logger = $logger;
        parent::__construct();
    }

    /**
     * @return string
     */
    public function getElasticaType()
    {
        return $this->ownerBaseClass;
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
        $db = $this->owner->inheritedDatabaseFields();

        //get fields details for searchable_fields of pagetype

        $additionalFields = array();

        if ($this->owner->has_extension('FileExtension')) {
            $additionalFields = $this->owner->additionalSearchableFields();
        }

        $fields = $this->owner->searchableFields() + $additionalFields;
        $result = array();

        foreach ($fields as $name => $params) {
            $type = null;
            $spec = array();


            if (array_key_exists($name, $db)) {
                $class = $db[$name];

                if (($pos = strpos($class, '('))) {
                    $class = substr($class, 0, $pos);
                }

                if (array_key_exists($class, self::$mappings)) {
                    $spec['type'] = self::$mappings[$class];
                }
            }
            elseif ($name == 'FileContent') { //handle File Contents
                $spec['type'] = 'attachment';
            }

            $result[$name] = $spec;
        }

        //DO to exclude
        $excludedDataObjects = [
            'BackLinkTracking' => 0,
            'LinkTracking' => 1,
            'Submissions' => 2,
            'CustomRecipientRules' => 3,
            'EmailRecipients' => 4,
            'WorkflowDefinition' => 5,
            'Parent' => 6,
            'ViewerGroups' => 7,
            'EditorGroups' => 8,
        ];

        //now loop through DataObjects related to $this->owner and get all searchable fields of those DO
        foreach (array($this->owner->has_many(), $this->owner->has_one(), $this->owner->many_many()) as $relationship) {
            foreach ($relationship as $data_object_ref => $data_object_classname) {
                if ($this->owner->$data_object_ref() instanceof \ArrayAccess) {

                    foreach ($this->owner->$data_object_ref() as $dataObject) {
                        $db = \DataObject::database_fields(get_class($dataObject));

                        $fields = $dataObject->searchableFields();

                        foreach ($fields as $name => $params) {
                            $type = null;
                            $spec = array();

                            if (array_key_exists($name, $db)) {
                                $class = $db[$name];

                                if (($pos = strpos($class, '('))) {
                                    $class = substr($class, 0, $pos);
                                }

                                if (array_key_exists($class, self::$mappings)) {

                                    $spec['type'] = self::$mappings[$class];
                                }
                            }
                            if(!array_key_exists($data_object_ref, $excludedDataObjects)) {
                                $result['DataObject_' . $data_object_ref . '_' . $name] = $spec;
                            }
                        }
                    }
                }

            }
        }
        $result['LastEdited'] = ['type' => 'date'];
        return $result;

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
            //handle the DataObjects
            if (substr($field, 0, 11) === "DataObject_") {

                $explosion = explode("_", $field);
                $class = $explosion[1];
                $dataObjectField = $explosion[2];
                $fieldArrayIndex = 'DataObject_' . $class . '_' . $dataObjectField;

                foreach ($this->owner->$class() as $dataObjectClass) {

                    if (!isset($fields[$fieldArrayIndex])) {
                        $fields[$fieldArrayIndex] = '';
                    }

                    $fields[$fieldArrayIndex] .= ' ' .$dataObjectClass->$dataObjectField;
                }

            }
            elseif ($field == 'FileContent') { //handle files
                $fields[$field] = base64_encode(file_get_contents($this->owner->getFullPath()));
            }
            elseif ($field == 'LastEdited') { //handle Last_Edited field
                //transform into valid date field according to elastica, otherwise it complains

                if ($this->owner->$field) {
                    $date = str_replace(' ', 'T', $this->owner->$field);

                    if ($date == '0000-00-00T00:00:00') {
                        $fields[$field] = date("Y-m-d");
                    } else {
                        $fields[$field] = $date;
                    }
                }
            }
            else { //handle regular fields from PageTypes
                $fields[$field] = $this->owner->$field;
            }

        }

        return new Document($this->owner->ID, $fields);
    }

    /**
     * Updates the record in the search index.
     */
    public function onAfterWrite()
    {

        if ($this->owner->ShowInSearch) {
            $this->service->index($this->owner);
        }
        else
        { //remove from index if should not be shown in search
            try {
                $this->service->remove($this->owner);
            } catch (NotFoundException $e) {
                if ($this->logger) {
                    $this->logger->log($e->getMessage());
                }
            }


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
