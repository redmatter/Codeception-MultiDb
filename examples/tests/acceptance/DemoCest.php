<?php

namespace Redmatter\Codeception\Tests;

use \AcceptanceTester;
use Faker\Factory as FakerFactory;

class DemoCest
{
    private $faker;

    public function __construct()
    {
        $this->faker = FakerFactory::create('en_GB');
    }

    public function demo(AcceptanceTester $I)
    {
        $I->amConnectedToDb('Primary');

        list($organisation_id, $user_id) = $I->transaction(function () use ($I) {
            $organisation_id = $this->createOrganisation($I);
            $user_id = $this->createUser($I, ['OrganisationID' => $organisation_id]);
            $this->createUserPassword($I, $user_id);

            return [$organisation_id, $user_id];
        });

        $I->amConnectedToDb('Secondary');
        $this->createAudit($I, $organisation_id, null, ['Description'=>'Organisation created']);
        $this->createAudit($I, $organisation_id, $user_id, ['Description'=>'User created and password set']);
    }

    private function createOrganisation(AcceptanceTester $I, array $details = [])
    {
        return $I->haveInDb('DemoConfig.Organisation', array_merge([
            'Name' => $this->faker->name.' & Co. Ltd.',
            'Address' => str_replace("\n", ', ', $this->faker->address),
            'Active' => 'YES'
        ], $details));
    }

    private function createUser(AcceptanceTester $I, array $details = [])
    {
        return $I->haveInDb('DemoConfig.User', array_merge([
            'Name' => $this->faker->name,
            'Email' => $this->faker->email,
            'Address' => str_replace("\n", ', ', $this->faker->address),
            'Active' => 'YES'
        ], $details));
    }

    private function createUserPassword(AcceptanceTester $I, $user_id, $password = null)
    {
        if ($password === null) {
            $password = $this->faker->password;
        }

        $I->haveInDb('DemoConfig.UserPassword', [
            'UserID' => $user_id,
            'Hash' => sha1($password, true),
            'CreatedAt' => '@asis NOW()',
            'ExpiresAt' => '@asis DATE_ADD(NOW(), INTERVAL 1 WEEK)'
        ], 'UserID', $user_id);
    }

    private function createAudit(AcceptanceTester $I, $organisation_id, $user_id, array $data)
    {
        $I->haveInDb('DemoWarehouse.Audit', [
            'OrganisationID'=>$organisation_id,
            'UserID'=>$user_id,
            'JSON'=>json_encode($data),
            'Time'=>'@asis NOW()'
        ], 'OrganisationID', $organisation_id);
    }
}
