SilverStripe Elastica Module
============================

Provides elastic search integration for SilverStripe DataObjects using Elastica.

Usage
-----

The first step is to configure the Elastic Search service. To do this, the configuration system
is used. The simplest default configuration (i.e. for `mysite/_config/injector.yml`) is:

    Injector:
      SilverStripe\Elastica\ElasticaService:
        constructor:
          - %$Elastica\Client
          - index-name-to-use

You can then use the `SilverStripe\Elastica\Searchable` extension to add searching functionality
to your data objects.

You could for example add the following code to `mysite/_config/injector.yml`:

    SiteTree:
      extensions:
        - 'SilverStripe\Elastica\Searchable'

Elasticsearch can then be interacted with using the `SilverStripe\Elastica\ElasticService` class.

To add special fields to the index, just update $searchable_fields for an object:

    class SomePage extends Page
    {
        private static $db = array(
            "SomeField1" => "Varchar(255)",
            "SomeField2"  => "Varchar(255)"
        );
        private static $searchable_fields = array(
            "SomeField1",
            "SomeField2"
        );
    }

After every change to your data model you should execute the `SilverStripe-Elastica-ReindexTask`:

    php framework/cli-script.php dev/tasks/SilverStripe-Elastica-ReindexTask
