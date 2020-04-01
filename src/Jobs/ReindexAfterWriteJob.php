<?php

namespace Heyday\Elastica\Jobs;

use SilverStripe\CMS\Model\SiteTree;
use SilverStripe\Core\Injector\Injector;
use SilverStripe\ORM\DataList;
use SilverStripe\ORM\DataObject;
use SilverStripe\Versioned\Versioned;
use Symbiote\QueuedJobs\Services\AbstractQueuedJob;
use Symbiote\QueuedJobs\Services\QueuedJob;

/**
 * Class ReindexAfterWriteJob
 * @package Heyday\Elastica\Jobs
 */
class ReindexAfterWriteJob extends AbstractQueuedJob implements QueuedJob
{

    /**
     *
     * get the instance to reindex and the service
     * ReindexAfterWriteJob constructor.
     * @param int $id
     * @param string $class
     */
    public function __construct($id = null, $class = null)
    {
        if ($id) {
            $this->id = $id;
        }
        if ($class) {
            $this->class = $class;
        }
    }


    /**
     * Defines the title of the job
     *
     * @return string
     */
    public function getTitle()
    {
        return "Reindexing " . $this->class . " ID " . $this->id;
    }

    /**
     * Indicate to the system which queue we think we should be in based
     * on how many objects we're going to touch on while processing.
     *
     * We want to make sure we also set how many steps we think we might need to take to
     * process everything - note that this does not need to be 100% accurate, but it's nice
     * to give a reasonable approximation
     *
     * @return int
     */
    public function getJobType()
    {
        $this->totalSteps = 'Lots';
        return QueuedJob::QUEUED;
    }

    /**
     * Lets process
     */
    public function process()
    {

        if (!$this->id || !$this->class) {
            // No valid data to index
            return;
        }

        $service = Injector::inst()->get('Heyday\Elastica\ElasticaService');
        $reading_mode = Versioned::get_reading_mode();
        Versioned::set_reading_mode('Stage.Live');
        $versionToIndex = DataObject::get($this->class)->byID($this->id);

        if (!$versionToIndex) {
            // No live version of the record to index
            return;
        }

        if (!$versionToIndex->hasField('ShowInSearch') || $versionToIndex->ShowInSearch) {
            $service->index($versionToIndex);
        } else {
            $service->remove($versionToIndex);
        }

        $this->updateDependentClasses($versionToIndex, $service);
        Versioned::set_reading_mode($reading_mode);
        $this->isComplete = true;
    }

    /**
     * Updates the records of all instances of dependent classes.
     */
    protected function updateDependentClasses($versionToIndex, $service)
    {
        $classes = $this->dependentClasses($versionToIndex);
        if ($classes) {
            foreach ($classes as $class) {
                $list = DataList::create($class);

                foreach ($list as $object) {
                    if ($object instanceof DataObject &&
                        $object->hasExtension('Heyday\\Elastica\\Searchable')
                    ) {
                        if (($object instanceof SiteTree && $object->ShowInSearch) ||
                            (!$object instanceof SiteTree)
                        ) {
                            $service->index($object);
                        } else {
                            $service->remove($object);
                        }
                    }
                }
            }
        }
    }

    /**
     * Return an array of dependant class names. These are classes that need to be reindexed when an instance of the
     * extended class is updated or when a relationship to it changes.
     * @return array|\scalar
     */
    public function dependentClasses($versionToIndex)
    {
        return $versionToIndex->config()->get('dependent_classes');
    }
}
