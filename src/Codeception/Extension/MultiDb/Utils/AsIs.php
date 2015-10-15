<?php
/**
 * @copyright 2009-2014 Red Matter Ltd (UK)
 */

namespace Codeception\Extension\MultiDb\Utils;

class AsIs
{
    protected $sql_fragment;

    /**
     * @param string $sql_fragment
     */
    public function __construct($sql_fragment)
    {
        $this->sql_fragment = $sql_fragment;
    }

    public function __toString()
    {
        return $this->sql_fragment;
    }
}
