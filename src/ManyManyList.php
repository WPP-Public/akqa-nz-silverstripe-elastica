<?php

namespace Heyday\Elastica;

use Exception;
use SilverStripe\ORM\DataList;
use SilverStripe\ORM\DataObject;
use SilverStripe\ORM\ManyManyList as SilverStripeManyManyList;

/**
 * A drop in replacement for the default ManyManyList.
 * This class allows us to invalidate records when many many relations change.
 *
 * @package Heyday\Elastica\SilverStripe
 */
class ManyManyList extends SilverStripeManyManyList
{
    /**
     * @param  mixed $item
     * @param  array $extraFields
     * @throws Exception
     */
    public function add($item, $extraFields = [])
    {
        parent::add($item, $extraFields);

        if (!$item instanceof DataObject) {
            $item = DataList::create($this->dataClass)->byId($item);
        }

        if ($item instanceof DataObject) {
            $item->extend('onAfterManyManyRelationAdd');
        }
    }

    /**
     * @param int $itemID
     */
    public function removeByID($itemID)
    {
        $result = parent::removeByID($itemID);

        $item = DataList::create($this->dataClass)->byId($itemID);

        if ($item instanceof $this->dataClass) {
            $item->extend('onAfterManyManyRelationRemove');
        }

        return $result;
    }

    public function removeAll()
    {
        parent::removeAll();

        $items = DataList::create($this->dataClass);
        /**
 * @var DataObject $item
*/
        foreach ($items as $item) {
            $item->extend('onAfterManyManyRelationRemove');
        }
    }
}
