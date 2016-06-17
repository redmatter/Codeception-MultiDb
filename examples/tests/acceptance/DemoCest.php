<?php

namespace Natterbox\Codeception\Tests;

use \AcceptanceTester;

class DemoCest
{
    public function tryToTest(AcceptanceTester $I)
    {
        $I->amConnectedToDb('Primary');

        $user_id = $I->transaction(function () use ($I) {
            $I->executeSql('SELECT NOW()');
            $user_id = $I->haveInDb('DemoConfig.User', [
                'OrganisationID'=>1,
                'Name'=>'Some One',
                'Email'=>'someone@example.com',
                'Address'=>'PO Box 1213, London, United Kingdom',
                'Active'=>'YES',
            ]);
            $I->haveInDb('DemoConfig.UserPassword', [
                'UserID'=>$user_id,
                'Hash'=>'@asis UNHEX(SHA1("topsecret"))',
                'CreatedAt'=>'@asis NOW()',
                'ExpiresAt'=>'@asis DATE_ADD(CreatedAt, INTERVAL 10 DAY)'
            ]);
            
            return $user_id;
        });

        $I->amConnectedToDb('Secondary');
        codecept_debug(
            $I->getFromDb('DemoWarehouse.Audit', ['OrganisationID'=>1])
        );
    }
}
