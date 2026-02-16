<?php

namespace Heyday\Elastica;

use SilverStripe\Dev\BuildTask;
use SilverStripe\PolyExecution\PolyOutput;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;

/**
 * Defines and refreshes the elastic search index.
 */
class ReindexTask extends BuildTask
{
    protected static string $commandName = 'elastica-reindex';

    protected string $title = 'Elastic Search Reindex';

    protected static string $description = 'Refreshes the elastic search index';

    /**
     * @var ElasticaService
     */
    private $service;

    /**
     * ReindexTask constructor.
     *
     * @param ElasticaService $service
     */
    public function __construct(ElasticaService $service)
    {
        parent::__construct();
        $this->service = $service;
    }

    /**
     * Defines (creates and defines mappings for) the index and refreshes the index content.
     *
     * You can delete the index before recreating it by adding `--recreate` option, which can help
     * when switching mapping types in your DataObject configuration.
     */
    protected function execute(InputInterface $input, PolyOutput $output): int
    {
        $output->writeln('Defining the mappings');
        $recreate = (bool) $input->getOption('recreate');
        $this->service->define($recreate);

        $output->writeln('Refreshing the index');
        $this->service->refresh();

        return Command::SUCCESS;
    }

    public function getOptions(): array
    {
        return [
            new InputOption(
                'recreate',
                null,
                InputOption::VALUE_NONE,
                'Delete and recreate the index before reindexing'
            ),
        ];
    }
}
