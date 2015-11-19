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
            $user_id = $I->haveInDb('PrimaryDb.User', [
                'UserName'=>'someone@example.com',
                'Name'=>'Some One',
                'CreatedAt'=>'@asis NOW()'
            ]);
            $I->haveInDb('PrimaryDb.UserPassword', [
                'UserID'=>$user_id,
                'Password'=>'topsecret',
                'CreatedAt'=>'@asis NOW()'
            ]);
            
            return $user_id;
        });
    }
}
