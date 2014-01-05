<?php

require_once(dirname(__DIR__).'/vendor/autoload.php');

use Zurau\Zurau;

$config = parse_ini_file(dirname(__DIR__).'/config/config.ini');

$zurau = new Zurau($config);

$zurau->metadata('test');

/*
$zurau->send();
$zurau->fetch();
$zurau->offsets();
$zurau->offset_commit();
$zurau->offset_fetch();
*/
