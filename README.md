SilverStripe Elastica Module
============================

Provides elastic search integration for SilverStripe DataObjects using Elastica.

Usage
-----

The first step is to configure the Elastic Search service. To do this, the configuration system
is used. The simplest default configuration is:

    Injector:
      SilverStripe\Elastica\ElasticaService:
        constructor:
          - %$Elastica\Client
          - index-name-to-use

You cna then use the `SilverStripe\Elastica\Searchable` extension to add searching functionality
to your data objects. Elastic search can then be interacted with using the
`SilverStripe\Elastica\ElasticService` class.
