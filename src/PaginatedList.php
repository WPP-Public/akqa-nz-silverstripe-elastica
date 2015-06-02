<?php

namespace Heyday\Elastica;

class PaginatedList extends \PaginatedList
{
    /**
     * Returns the total number of items in the unpaginated list.
     *
     * @return int
     */
    public function getTotalItems() {
        if ($this->totalItems === null) {
            $this->totalItems = $this->getResults()->getTotalHits();
        }

        return $this->totalItems;
    }
} 