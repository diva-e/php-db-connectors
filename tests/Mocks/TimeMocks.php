<?php
// here we replace the builtin gmdate function, which is used within the Divae\DbConnectors\TempTableHandler namespace
/** @noinspection PhpIllegalPsrClassPathInspection */
namespace Divae\DbConnectors\TempTableHandler;

function gmdate(string $format): string {
    if (TimeMocks::$enabled) {
        return TimeMocks::$gmdate;
    }
    else {
        return \gmdate($format);
    }
}

abstract class TimeMocks {
    public static bool $enabled = false;
    public static string $gmdate = "";
}
