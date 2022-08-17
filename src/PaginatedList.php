<?php

namespace Heyday\Elastica;

/**
 * Class PaginatedList
 *
 * @package Heyday\Elastica
 */
class PaginatedList extends \SilverStripe\ORM\PaginatedList
{
    /**
     * Use the ResultList's total items method to determine this value
     *
     * @return int
     */
    public function getTotalItems()
    {
        if ($this->list instanceof ResultList) {
            return $this->list->getTotalItems();
        }

        return parent::getTotalItems();
    }


    public function FirstItem()
    {
        if ($this->list instanceof ResultList) {
            return $this->list->getFirstItem();
        }

        return parent::getTotalItems();
    }


    public function LastItem()
    {
        if ($this->list instanceof ResultList) {
            return $this->list->getLastItem();
        }

        return parent::getTotalItems();
    }
}
