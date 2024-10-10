<?php

namespace Heyday\Elastica;

use SilverStripe\Control\HTTPRequest;
use \SilverStripe\ORM\PaginatedList as SilverStripePaginatedList;
/**
 * Class PaginatedList
 *
 * @package Heyday\Elastica
 */
class PaginatedList extends SilverStripePaginatedList
{
    protected $resultList;

    public function __construct(ResultList $resultList, $request = [])
    {
        $this->resultList = $resultList;
        $this->setRequest($request);

        $start = 0;

        if ($request instanceof HTTPRequest) {
            if ($request->getVar('start') !== null) {
                $start = (int) $request->getVar('start');
            }
        }

        $this->list = $resultList->limit($this->getPageLength(), $start)->toArrayList();

        $list = new SilverStripePaginatedList($this->list, $request);

        parent::__construct($list, $request);

        $this->setTotalItems($resultList->getTotalItems());
        $this->setPageStart($this->getPageStart());
        $this->setLimitItems(false);
    }


    public function setPageStart($start)
    {
        $this->list = $this->resultList->limit($this->getPageLength(), $start)->toArrayList();

        return $this;
    }


    public function setPageLength($length)
    {
        parent::setPageLength($length);
        $this->list = $this->resultList->limit($length, $this->getPageStart())->toArrayList();

        return $this;
    }


    /**
     * Use the ResultList's total items method to determine this value
     *
     * @return int
     */
    public function getTotalItems()
    {
        if ($this->resultList instanceof ResultList) {
            return $this->resultList->getTotalItems();
        }

        return parent::getTotalItems();
    }
}
