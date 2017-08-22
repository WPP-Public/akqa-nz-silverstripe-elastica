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
 * Created by PhpStorm.
 * User: bdubuisson
 * Date: 9/08/17
 * Time: 10:06 AM
 */
class ReindexAfterWriteJob extends AbstractQueuedJob implements QueuedJob
{

    /**
     *
     * get the instance to reindex and the service
     * ReindexAfterWriteJob constructor.
     * @param null $owner
     */
    public function __construct($owner = null)
    {
        if ($owner) {
            $this->owner = $owner;
        }

        $this->service = Injector::inst()->get('Heyday\Elastica\ElasticaService');
    }


    /**
     * Defines the title of the job
     *
     * @return string
     */
    public function getTitle()
    {
        return "Reindexing " . $this->owner->ClassName . " ID " . $this->owner->ID;
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
        if ($this->owner && $this->service) {
            $reading_mode = Versioned::get_reading_mode();
            Versioned::set_reading_mode('Stage.Live');

            $versionToIndex = DataObject::get($this->owner->ClassName)->byID($this->owner->ID);
            if (is_null($versionToIndex)) {
                $versionToIndex = $this->owner;
            }

            if (($versionToIndex instanceof SiteTree && $versionToIndex->ShowInSearch) ||
                (!$versionToIndex instanceof SiteTree && ($versionToIndex->hasMethod('getShowInSearch') && $versionToIndex->ShowInSearch)) ||
                (!$versionToIndex instanceof SiteTree && !$versionToIndex->hasMethod('getShowInSearch'))
            ) {
                $this->service->index($versionToIndex);
            } else {
                $this->service->remove($versionToIndex);
            }

            $this->updateDependentClasses();

            Versioned::set_reading_mode($reading_mode);
            $this->isComplete = true;
            return;
        } else {
            return;
        }
    }

    /**
     * Updates the records of all instances of dependent classes.
     */
    protected function updateDependentClasses()
    {
        $classes = $this->dependentClasses();
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
                            $this->service->index($object);
                        } else {
                            $this->service->remove($object);
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
    public function dependentClasses()
    {
        return $this->owner->stat('dependent_classes');
    }
}
