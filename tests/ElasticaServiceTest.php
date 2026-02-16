<?php

namespace Heyday\Elastica\Tests;

use Elastica\Index;
use Heyday\Elastica\ElasticaService;
use PHPUnit\Framework\MockObject\MockObject;
use SilverStripe\Dev\SapphireTest;

class ElasticaServiceTest extends SapphireTest
{
    public function testDefineDeletesIndexIfRecreateIsPassed(): void
    {
        /** @var ElasticaService&MockObject $service */
        $service = $this->getMockBuilder(ElasticaService::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['getIndex', 'createIndex', 'getIndexedClasses'])
            ->getMock();

        $service->expects($this->once())->method('getIndexedClasses')->willReturn([]);

        /** @var Index&MockObject $index */
        $index = $this->createMock(Index::class);
        $index->expects($this->once())->method('exists')->willReturn(true);
        $index->expects($this->once())->method('delete');

        $service->expects($this->once())->method('getIndex')->willReturn($index);
        $service->expects($this->once())->method('createIndex');

        $service->define(true);
    }
}
