<?php

namespace Heyday\Elastica;

use SilverStripe\Control\Director;
use SilverStripe\Control\HTTPRequest;
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
     * Defines (creates and defines mappings for) the index and refreshes the index content.
     *
     * You can delete the index before recreating it by adding `recreate=1` as a request argument, which can help
     * when switching mapping types in your DataObject configuration.
     *
     * @param HTTPRequest $request
     */
    public function run($request)
    {
        $message = function ($content) {
            print(Director::is_cli() ? "$content\n" : "<p>$content</p>");
        };

        $message('Defining the mappings');
        $recreate = (bool) $request->getVar('recreate');
        $this->service->define($recreate);

        $message('Refreshing the index');
        $this->service->refresh();
    }
}
