<?php

namespace Natterbox\Codeception\Tests;

use \AcceptanceTester;

class DemoCest
{
    public function tryToTest(AcceptanceTester $I)
    {
        $I->amConnectedToDb('Primary');

        $I->transaction(function () use ($I) {
            $I->executeSql('SELECT NOW()');
        });
    }
}
