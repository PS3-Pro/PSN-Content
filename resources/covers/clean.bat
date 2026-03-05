@echo off
setlocal EnableDelayedExpansion
title Limpador de Capas (Compressed e Uncompressed)
color 0A

echo ======================================================
echo  VERIFICANDO ARQUIVOS EM 'COMPRESSED' E 'UNCOMPRESSED'
echo ======================================================

:: 1. CRIA A LISTA TEMPORARIA
(
echo BCAS01252.JPG
echo BCAS01741.JPG
echo BCAS31156.JPG
echo BCAX90001.JPG
echo BCES00018.JPG
echo BCES00943.JPG
echo BCES06099.JPG
echo BCES30580.JPG
echo BCES98119.JPG
echo BCES98125.JPG
echo BCET00027.JPG
echo BCJB95007.JPG
echo BCJX96005.JPG
echo BCUN00001.JPG
echo BCUS00511.JPG
echo BCUS00850.JPG
echo BCUS00908.JPG
echo BCUS90762.JPG
echo BDPR01002.JPG
echo BJES01709.JPG
echo BLAS30407.JPG
echo BLAS31322.JPG
echo BLAS50455.JPG
echo BLBC20001.JPG
echo BLBL00001.JPG
echo BLBM00002.JPG
echo BLED00339.JPG
echo BLED00505.JPG
echo BLED00702.JPG
echo BLEP00001.JPG
echo BLES00001.JPG
echo BLES00022.JPG
echo BLES00031.JPG
echo BLES00051.JPG
echo BLES00096.JPG
echo BLES00099.JPG
echo BLES00393.JPG
echo BLES00484.JPG
echo BLES01253.JPG
echo BLES01435.JPG
echo BLES01806.JPG
echo BLES01GTA.JPG
echo BLES06294.JPG
echo BLES13370.JPG
echo BLES17022.JPG
echo BLES17780.JPG
echo BLES30383.JPG
echo BLES30538.JPG
echo BLES30566.JPG
echo BLES30645.JPG
echo BLES30682.JPG
echo BLES30798.JPG
echo BLES30810.JPG
echo BLES30855.JPG
echo BLES30927.JPG
echo BLES30985.JPG
echo BLES31063.JPG
echo BLES60111.JPG
echo BLES61001.JPG
echo BLES61008.JPG
echo BLES61010.JPG
echo BLES61023.JPG
echo BLES61039.JPG
echo BLES61040.JPG
echo BLES61195.JPG
echo BLES71195.JPG
echo BLES80609.JPG
echo BLES80610.JPG
echo BLES80611.JPG
echo BLES81195.JPG
echo BLES88888.JPG
echo BLES90506.JPG
echo BLES99999.JPG
echo BLESBLUS3.JPG
echo BLEU30554.JPG
echo BLEX00001.JPG
echo BLJL61019.JPG
echo BLJM60545.JPG
echo BLJM85011.JPG
echo BLJMXXXXX.JPG
echo BLJS00178.JPG
echo BLJS30847.JPG
echo BLJX94002.JPG
echo BLJX94004.JPG
echo BLJX94009.JPG
echo BLKS20196.JPG
echo BLKT99001.JPG
echo BLKT99002.JPG
echo BLKT99003.JPG
echo BLKT99004.JPG
echo BLKT99005.JPG
echo BLKT99006.JPG
echo BLNG20001.JPG
echo BLRD00001.JPG
echo BLT600002.JPG
echo BLUD80001.JPG
echo BLUD80019.JPG
echo BLUD90004.JPG
echo BLUE01378.JPG
echo BLUS00305.JPG
echo BLUS00659.JPG
echo BLUS00875.JPG
echo BLUS00942.JPG
echo BLUS01066.JPG
echo BLUS01163.JPG
echo BLUS01231.JPG
echo BLUS01287.JPG
echo BLUS01408.JPG
echo BLUS01462.JPG
echo BLUS01676.JPG
echo BLUS01702.JPG
echo BLUS01796.JPG
echo BLUS01807.JPG
echo BLUS01921.JPG
echo BLUS01935.JPG
echo BLUS03727.JPG
echo BLUS30860.JPG
echo BLUS31750.JPG
echo BLUS80646.JPG
echo BOOT04XXX.JPG
echo BRAZ00000.JPG
echo BRAZ00003.JPG
echo BRAZ00004.JPG
echo BRAZ00006.JPG
echo BRAZ00013.JPG
echo BRAZ00015.JPG
echo BRAZ00017.JPG
echo BROWSER01.JPG
echo CAPPSL123.JPG
echo CBRA00000.JPG
echo CEX2DEX00.JPG
echo CNDR00003.JPG
echo CONDR0000.JPG
echo CPS239940.JPG
echo DUMP80608.JPG
echo EGDL00001.JPG
echo FAK009922.JPG
echo FBAL00123.JPG
echo FBAN00001.JPG
echo FBAN00003.JPG
echo FFCC10000.JPG
echo FONV30001.JPG
echo GBAGAME01.JPG
echo GBAGAME02.JPG
echo GBAGAME03.JPG
echo GBAGAME04.JPG
echo GBAGAME05.JPG
echo GDSW00001.JPG
echo GLES00246.JPG
echo GPKG00123.JPG
echo HELL12345.JPG
echo HLES01069.JPG
echo LAUN12345.JPG
echo LGES00546.JPG
echo LGME00001.JPG
echo LGRLFAN00.JPG
echo MARK00095.JPG
echo MONKEY200.JPG
echo MRMJ00002.JPG
echo NCUS19001.JPG
echo NOCD00229.JPG
echo NPEA00057.JPG
echo NPEA00135.JPG
echo NPEA00309.JPG
echo NPEA00326.JPG
echo NPEA00327.JPG
echo NPEA80017.JPG
echo NPEA99999.JPG
echo NPEB00036.JPG
echo NPEB00158.JPG
echo NPEB00239.JPG
echo NPEB00416.JPG
echo NPEB00620.JPG
echo NPEB00645.JPG
echo NPEB00647.JPG
echo NPEB00651.JPG
echo NPEB00787.JPG
echo NPEB00814.JPG
echo NPEB00867.JPG
echo NPEB00874.JPG
echo NPEB00913.JPG
echo NPEB01028.JPG
echo NPEB01114.JPG
echo NPEB01137.JPG
echo NPEB01138.JPG
echo NPEB01139.JPG
echo NPEB01222.JPG
echo NPEB01238.JPG
echo NPEB01301.JPG
echo NPEB01324.JPG
echo NPEB01373.JPG
echo NPEB01786.JPG
echo NPEB90343.JPG
echo NPEB90357.JPG
echo NPEB90360.JPG
echo NPEB90474.JPG
echo NPEB90522.JPG
echo NPED01283.JPG
echo NPHA80127.JPG
echo NPHB00453.JPG
echo NPHB00525.JPG
echo NPIA00005.JPG
echo NPJB00047.JPG
echo NPJB00188.JPG
echo NPJB00201.JPG
echo NPJB00231.JPG
echo NPJB00317.JPG
echo NPJB40002.JPG
echo NPJB90632.JPG
echo NPUA80132.JPG
echo NPUA80133.JPG
echo NPUA80788.JPG
echo NPUA80864.JPG
echo NPUB30009.JPG
echo NPUB30053.JPG
echo NPUB30073.JPG
echo NPUB30129.JPG
echo NPUB30159.JPG
echo NPUB30235.JPG
echo NPUB30308.JPG
echo NPUB30323.JPG
echo NPUB30384.JPG
echo NPUB30463.JPG
echo NPUB30470.JPG
echo NPUB30499.JPG
echo NPUB30515.JPG
echo NPUB30524.JPG
echo NPUB30536.JPG
echo NPUB30563.JPG
echo NPUB30581.JPG
echo NPUB30589.JPG
echo NPUB30603.JPG
echo NPUB30625.JPG
echo NPUB30646.JPG
echo NPUB30650.JPG
echo NPUB30652.JPG
echo NPUB30653.JPG
echo NPUB30654.JPG
echo NPUB30672.JPG
echo NPUB30676.JPG
echo NPUB30699.JPG
echo NPUB30714.JPG
echo NPUB30718.JPG
echo NPUB30719.JPG
echo NPUB30732.JPG
echo NPUB30733.JPG
echo NPUB30769.JPG
echo NPUB30780.JPG
echo NPUB30798.JPG
echo NPUB30817.JPG
echo NPUB30829.JPG
echo NPUB30856.JPG
echo NPUB30864.JPG
echo NPUB30869.JPG
echo NPUB30872.JPG
echo NPUB30888.JPG
echo NPUB30903.JPG
echo NPUB31064.JPG
echo NPUB31078.JPG
echo NPUB31079.JPG
echo NPUB31177.JPG
echo NPUB31221.JPG
echo NPUB31225.JPG
echo NPUB31260.JPG
echo NPUB31288.JPG
echo NPUB90443.JPG
echo NPUB90661.JPG
echo NPUB90925.JPG
echo NPUB90987.JPG
echo NPUZ00014.JPG
echo NPUZ00125.JPG
) > lista_apagar.txt

:: 2. CONTA O TOTAL DE ARQUIVOS NA LISTA
set total=0
for /f %%A in ('type lista_apagar.txt ^| find /c /v ""') do set total=%%A

echo.
echo Buscando em:
echo  - .\compressed\
echo  - .\uncompressed\
echo.
echo Total de NOMES para verificar: %total%
echo ------------------------------------------------------

:: 3. LOOP PARA APAGAR E CONTAR
set atual=0
set apagados_comp=0
set apagados_uncomp=0

for /f "delims=" %%i in (lista_apagar.txt) do (
    set /a atual+=1
    set "encontrou=0"
    set "msg="
    
    :: Verifica pasta compressed
    if exist "compressed\%%i" (
        del /f /q "compressed\%%i"
        set /a apagados_comp+=1
        set "msg= [Apagado de COMPRESSED]"
        set "encontrou=1"
    )

    :: Verifica pasta uncompressed
    if exist "uncompressed\%%i" (
        del /f /q "uncompressed\%%i"
        set /a apagados_uncomp+=1
        set "msg=!msg! [Apagado de UNCOMPRESSED]"
        set "encontrou=1"
    )

    if "!encontrou!"=="1" (
        echo [!atual!/%total%] %%i !msg!
    ) else (
        echo [!atual!/%total%] %%i - Nao encontrado
    )
)

:: 4. RELATORIO FINAL
del lista_apagar.txt
set /a total_geral=apagados_comp+apagados_uncomp

echo.
echo ==========================================
echo            RELATORIO FINAL
echo ==========================================
echo.
echo Arquivos apagados em 'compressed'  : !apagados_comp!
echo Arquivos apagados em 'uncompressed': !apagados_uncomp!
echo.
echo TOTAL GERAL DE EXCLUSOES: !total_geral!
echo.
pause