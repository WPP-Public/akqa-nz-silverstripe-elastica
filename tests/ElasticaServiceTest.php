<?php

namespace Heyday\Elastica\Tests;

use Elastica\Index;
use Heyday\Elastica\ElasticaService;
use PHPUnit_Framework_MockObject_MockObject;
use SilverStripe\Dev\SapphireTest;

class ElasticaServiceTest extends SapphireTest
{
    public function testDefineDeletesIndexIfRecreateIsPassed()
    {
        /**
 * @var ElasticaService|PHPUnit_Framework_MockObject_MockObject $service
*/
        $service = $this->getMockBuilder(ElasticaService::class)
            ->disableOriginalConstructor()
            ->setMethods(['getIndex', 'createIndex', 'getIndexedClasses'])
            ->getMock();

        $service->expects($this->once())->method('getIndexedClasses')->willReturn([]);

        /**
 * @var Index|PHPUnit_Framework_MockObject_MockObject $index
*/
        $index = $this->createMock(Index::class);
        $index->expects($this->exactly(2))->method('exists')->willReturnOnConsecutiveCalls(true, false);
        $index->expects($this->once())->method('delete');

        $service->expects($this->once())->method('getIndex')->willReturn($index);
        $service->expects($this->once())->method('createIndex');

        $service->define(true);
    }
}
