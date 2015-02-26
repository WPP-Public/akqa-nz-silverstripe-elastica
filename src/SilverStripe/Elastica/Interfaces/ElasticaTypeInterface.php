<?php

namespace SilverStripe\Elastica\Interfaces;

interface ElasticaTypeInterface
{
    /**
     * Returns the Elasticsearch Type for this document
     * @return mixed
     */
    public function getElasticaType();
}