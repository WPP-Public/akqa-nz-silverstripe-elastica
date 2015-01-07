<?php

namespace SilverStripe\Elastica;

use Elastica\Document;
use Elastica\Type\Mapping;

/**
 * Adds elastic search integration to a data object.
 */
class Searchable extends \DataExtension {

	public static $mappings = array(
		'Boolean'     => 'boolean',
		'Decimal'     => 'double',
		'Double'      => 'double',
		'Enum'        => 'string',
		'Float'       => 'float',
		'HTMLText'    => 'string',
		'Varchar(255)'=> 'string',
		'Varchar(50)' => 'string',
		'HTMLVarchar' => 'string',
		'Int'         => 'integer',
		'SS_Datetime' => 'date',
		'Text'        => 'string',
		'Varchar'     => 'string',
		'Year'        => 'integer'
	);

	private $service;

	public function __construct(ElasticaService $service) {
		$this->service = $service;
		parent::__construct();
	}

	/**
	 * @return string
	 */
	public function getElasticaType() {
		return $this->ownerBaseClass;
	}

	/**
	 * Gets an array of elastic field definitions.
	 *
	 * @return array
	 */
	public function getElasticaFields() {
		$db = \DataObject::database_fields(get_class($this->owner));
		$fields = $this->owner->searchableFields();
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

			$result[$name] = $spec;
		}
		//now loop through DataObject related to $this->owner and get all searchable fields of those DO
		foreach (array($this->owner->has_many(), $this->owner->has_one(), $this->owner->many_many()) as $relationship) {
			foreach ($relationship as $data_object_ref => $data_object_classname) {

				$to_ignore = ['Image', 'WorkflowDefinition', 'SiteTree', 'RateGroup', 'LibraryImage'];

				if (!in_array($data_object_classname, $to_ignore) && $this->owner->$data_object_ref()->first()) {
					$count = 1;
					foreach ($this->owner->$data_object_ref() as $dataObject) {
						$db = \DataObject::database_fields(get_class($dataObject));
//						var_dump($db);
//						die();

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

							$result['_DO_' . $data_object_ref . '_' . $count . '_' . $name] = $spec;
							$count++;
						}
					}
				}
			}
		}

//		var_dump($result);
//		die();
		return $result;
	}

	/**
	 * @return \Elastica\Type\Mapping
	 */
	public function getElasticaMapping() {
		$mapping = new Mapping();
		$mapping->setProperties($this->getElasticaFields());

		return $mapping;
	}

	public function getElasticaDocument() {
		$fields = array();

		foreach ($this->getElasticaFields() as $field => $config) {
			//handle the DataObjects
			if (substr($field, 0, 4 ) === "_DO_") {

				$explosion = explode("_", $field);
				$class = $explosion[2];
				$dataObjectField = $explosion[4];

				$count = 1;
				foreach ($this->owner->$class() as $dataObjectClass) {
					$fields['_DO_' . $class . '_' . $count . '_' . $dataObjectField] = $dataObjectClass->$dataObjectField;
					$count++;
				}

			}
			else {
				$fields[$field] = $this->owner->$field;
			}

		}

		return new Document($this->owner->ID, $fields);
	}

	/**
	 * Updates the record in the search index.
	 */
	public function onAfterWrite() {
		$this->service->index($this->owner);
	}

	/**
	 * Removes the record from the search index.
	 */
	public function onAfterDelete() {
		$this->service->remove($this->owner);
	}

}
