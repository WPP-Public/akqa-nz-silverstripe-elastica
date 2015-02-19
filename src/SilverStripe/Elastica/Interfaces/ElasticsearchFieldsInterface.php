<?php

namespace SilverStripe\Elastica\Interfaces;

interface ElasticSearchFieldsInterface
{
    /**
     * Returns an array of additional search fields not found in the searchableFields function.
     *
     * Usually used to map to method calls that cannot be put in the searchable_fields static
     * variable on the data object.
     *
     * Format: array('FieldName' => 'Type');
     *
     * FieldName can be a field in the database or a method name
     *
     * @return array
     */
    public function elasticSearchFields();
}