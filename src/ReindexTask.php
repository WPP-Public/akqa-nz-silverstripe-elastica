<?php

namespace Heyday\Elastica;

use SilverStripe\Control\Director;
use SilverStripe\Dev\BuildTask;

/**
 * Defines and refreshes the elastic search index.
 */
class ReindexTask extends BuildTask
{

    protected $title = 'Elastic Search Reindex';

    protected $description = 'Refreshes the elastic search index';

    /**
     * @var ElasticaService
     */
    private $service;

    /**
     * ReindexTask constructor.
     * @param ElasticaService $service
     */
    public function __construct(ElasticaService $service)
    {
        $this->service = $service;
    }

    /**
     * @param \SilverStripe\Control\HTTPRequest $request
     */
    public function run($request)
    {
        $message = function ($content) {
            print(Director::is_cli() ? "$content\n" : "<p>$content</p>");
        };

        $message('Defining the mappings');
        $this->service->define();

        $message('Refreshing the index');
        $this->service->refresh();
    }
}
