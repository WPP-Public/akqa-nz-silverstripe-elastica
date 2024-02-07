<?php

namespace Heyday\Elastica;

/**
 * Class PaginatedList
 *
 * @package Heyday\Elastica
 */
class PaginatedList extends \SilverStripe\ORM\PaginatedList
{
    protected $list;

    public function __construct(ResultList $list, $request = [])
    {
        $this->setRequest($request);
        $this->list = $list;
    }


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
