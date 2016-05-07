del classes\artifacts\manager.jar classes\artifacts\worker.jar classes\artifacts\package.zip

move classes\artifacts\manager\dsp-ass1.jar classes\artifacts\manager.jar

move classes\artifacts\worker\dsp-ass1.jar classes\artifacts\worker.jar

"c:\Program Files\WinRAR\WinRAR.exe" a -ep 	-afzip classes\artifacts\package.zip classes\artifacts\worker.jar classes\artifacts\manager.jar %HOMEPATH%\.aws\credentials -pbansko2016

exit