# Inventaire Des Routes

Inventaire compact des routes définies dans les manifests TOML à la racine du dépôt.

- Manifests couverts: **8**
- Entrées de routes totales: **310**
- `route_id` uniques: **122**
- Format: `route_id` | paire | `venue(pool)` -> `venue(pool)`

## `amm_12_pairs_fast.toml`

Nombre d'entrées: **24**.

- `sol-usdc-ray-3ucn-orca-czfq` | **SOL/USDC** | `raydium_clmm(3ucN...sUxv)` -> `orca_whirlpool(Czfq...44zE)`
- `sol-usdc-orca-czfq-ray-3ucn` | **SOL/USDC** | `orca_whirlpool(Czfq...44zE)` -> `raydium_clmm(3ucN...sUxv)`
- `usdt-sol-ray-3nmf-orca-fwew` | **USDT/SOL** | `raydium_clmm(3nMF...qEgF)` -> `orca_whirlpool(Fwew...qqGC)`
- `usdt-sol-orca-fwew-ray-3nmf` | **USDT/SOL** | `orca_whirlpool(Fwew...qqGC)` -> `raydium_clmm(3nMF...qEgF)`
- `usdt-usdc-ray-bztg-orca-4fuu` | **USDT/USDC** | `raydium_clmm(BZtg...8mUU)` -> `orca_whirlpool(4fuU...y4T4)`
- `usdt-usdc-orca-4fuu-ray-bztg` | **USDT/USDC** | `orca_whirlpool(4fuU...y4T4)` -> `raydium_clmm(BZtg...8mUU)`
- `jto-sol-ray-jvop-orca-2uhf` | **JTO/SOL** | `raydium_clmm(JVoP...uvZo)` -> `orca_whirlpool(2UhF...4gHJ)`
- `jto-sol-orca-2uhf-ray-jvop` | **JTO/SOL** | `orca_whirlpool(2UhF...4gHJ)` -> `raydium_clmm(JVoP...uvZo)`
- `trump-sol-ray-gqsp-orca-ckp1` | **TRUMP/SOL** | `raydium_clmm(GQsP...vNvW)` -> `orca_whirlpool(Ckp1...n7p2)`
- `trump-sol-orca-ckp1-ray-gqsp` | **TRUMP/SOL** | `orca_whirlpool(Ckp1...n7p2)` -> `raydium_clmm(GQsP...vNvW)`
- `trump-usdc-ray-7xzv-orca-6nd6` | **TRUMP/USDC** | `raydium_clmm(7XzV...9kuh)` -> `orca_whirlpool(6nD6...XigJ)`
- `trump-usdc-orca-6nd6-ray-7xzv` | **TRUMP/USDC** | `orca_whirlpool(6nD6...XigJ)` -> `raydium_clmm(7XzV...9kuh)`
- `jup-sol-ray-ezvk-orca-c1mg` | **JUP/SOL** | `raydium_clmm(EZVk...1qYw)` -> `orca_whirlpool(C1Mg...W8Wz)`
- `jup-sol-orca-c1mg-ray-ezvk` | **JUP/SOL** | `orca_whirlpool(C1Mg...W8Wz)` -> `raydium_clmm(EZVk...1qYw)`
- `jup-usdc-ray-fu5u-orca-4ui9` | **JUP/USDC** | `raydium_clmm(Fu5u...DuFr)` -> `orca_whirlpool(4Ui9...3szP)`
- `jup-usdc-orca-4ui9-ray-fu5u` | **JUP/USDC** | `orca_whirlpool(4Ui9...3szP)` -> `raydium_clmm(Fu5u...DuFr)`
- `ray-sol-ray-2axx-orca-d3c5` | **RAY/SOL** | `raydium_clmm(2AXX...rvY2)` -> `orca_whirlpool(D3C5...7tet)`
- `ray-sol-orca-d3c5-ray-2axx` | **RAY/SOL** | `orca_whirlpool(D3C5...7tet)` -> `raydium_clmm(2AXX...rvY2)`
- `ray-usdc-ray-61r1-orca-a2j7` | **RAY/USDC** | `raydium_clmm(61R1...C8ht)` -> `orca_whirlpool(A2J7...od1A)`
- `ray-usdc-orca-a2j7-ray-61r1` | **RAY/USDC** | `orca_whirlpool(A2J7...od1A)` -> `raydium_clmm(61R1...C8ht)`
- `msol-sol-ray-8ezb-orca-hqcy` | **MSOL/SOL** | `raydium_clmm(8Ezb...MjJ3)` -> `orca_whirlpool(HQcY...b8cx)`
- `msol-sol-orca-hqcy-ray-8ezb` | **MSOL/SOL** | `orca_whirlpool(HQcY...b8cx)` -> `raydium_clmm(8Ezb...MjJ3)`
- `psol-sol-ray-7pqk-orca-3xlk` | **PSOL/SOL** | `raydium_clmm(7PQk...t23Z)` -> `orca_whirlpool(3XLk...WMBT)`
- `psol-sol-orca-3xlk-ray-7pqk` | **PSOL/SOL** | `orca_whirlpool(3XLk...WMBT)` -> `raydium_clmm(7PQk...t23Z)`

## `amm_cross_venue_generated_fast.toml`

Nombre d'entrées: **40**.

- `jlp-sol-ray-8kbr-orca-6a3m` | **JLP/SOL** | `raydium_clmm(8Kbr...4U7Q)` -> `orca_whirlpool(6a3m...xYgd)`
- `jlp-sol-orca-6a3m-ray-8kbr` | **JLP/SOL** | `orca_whirlpool(6a3m...xYgd)` -> `raydium_clmm(8Kbr...4U7Q)`
- `wbtc-sol-ray-hcfy-orca-b5ew` | **WBTC/SOL** | `raydium_clmm(HCfy...KDdA)` -> `orca_whirlpool(B5Ew...4PQA)`
- `wbtc-sol-orca-b5ew-ray-hcfy` | **WBTC/SOL** | `orca_whirlpool(B5Ew...4PQA)` -> `raydium_clmm(HCfy...KDdA)`
- `usx-usdc-ray-ewiv-orca-2e3w` | **USX/USDC** | `raydium_clmm(EWiv...Prp6)` -> `orca_whirlpool(2e3W...Ybix)`
- `usx-usdc-orca-2e3w-ray-ewiv` | **USX/USDC** | `orca_whirlpool(2e3W...Ybix)` -> `raydium_clmm(EWiv...Prp6)`
- `geod-usdc-ray-14ik-orca-ig9e` | **GEOD/USDC** | `raydium_clmm(14ik...Eey1)` -> `orca_whirlpool(ig9E...6QkC)`
- `geod-usdc-orca-ig9e-ray-14ik` | **GEOD/USDC** | `orca_whirlpool(ig9E...6QkC)` -> `raydium_clmm(14ik...Eey1)`
- `usdt-usdc-ray-bztg-orca-4fuu` | **USDT/USDC** | `raydium_clmm(BZtg...8mUU)` -> `orca_whirlpool(4fuU...y4T4)`
- `usdt-usdc-orca-4fuu-ray-bztg` | **USDT/USDC** | `orca_whirlpool(4fuU...y4T4)` -> `raydium_clmm(BZtg...8mUU)`
- `eurc-usdc-ray-2zvv-orca-aris` | **EURC/USDC** | `raydium_clmm(2zVV...sK1w)` -> `orca_whirlpool(Aris...oZ9i)`
- `eurc-usdc-orca-aris-ray-2zvv` | **EURC/USDC** | `orca_whirlpool(Aris...oZ9i)` -> `raydium_clmm(2zVV...sK1w)`
- `sol-usdc-ray-3ucn-orca-czfq` | **SOL/USDC** | `raydium_clmm(3ucN...sUxv)` -> `orca_whirlpool(Czfq...44zE)`
- `sol-usdc-orca-czfq-ray-3ucn` | **SOL/USDC** | `orca_whirlpool(Czfq...44zE)` -> `raydium_clmm(3ucN...sUxv)`
- `tslax-usdc-ray-8ada-orca-9p7a` | **TSLAX/USDC** | `raydium_clmm(8aDa...NpFF)` -> `orca_whirlpool(9p7a...xokN)`
- `tslax-usdc-orca-9p7a-ray-8ada` | **TSLAX/USDC** | `orca_whirlpool(9p7a...xokN)` -> `raydium_clmm(8aDa...NpFF)`
- `nvdax-usdc-ray-49im-orca-6r4r` | **NVDAX/USDC** | `raydium_clmm(49iM...yyw6)` -> `orca_whirlpool(6R4r...TXCo)`
- `nvdax-usdc-orca-6r4r-ray-49im` | **NVDAX/USDC** | `orca_whirlpool(6R4r...TXCo)` -> `raydium_clmm(49iM...yyw6)`
- `spyx-usdc-ray-6tru-orca-fae5` | **SPYX/USDC** | `raydium_clmm(6tru...nDDE)` -> `orca_whirlpool(Fae5...CsHR)`
- `spyx-usdc-orca-fae5-ray-6tru` | **SPYX/USDC** | `orca_whirlpool(Fae5...CsHR)` -> `raydium_clmm(6tru...nDDE)`
- `crclx-usdc-ray-gyqh-orca-5thn` | **CRCLX/USDC** | `raydium_clmm(GYqH...yaFV)` -> `orca_whirlpool(5ThN...qsK3)`
- `crclx-usdc-orca-5thn-ray-gyqh` | **CRCLX/USDC** | `orca_whirlpool(5ThN...qsK3)` -> `raydium_clmm(GYqH...yaFV)`
- `gldx-usdc-ray-78re-orca-9cru` | **GLDX/USDC** | `raydium_clmm(78Re...hyze)` -> `orca_whirlpool(9crU...gLmx)`
- `gldx-usdc-orca-9cru-ray-78re` | **GLDX/USDC** | `orca_whirlpool(9crU...gLmx)` -> `raydium_clmm(78Re...hyze)`
- `msol-usdc-ray-gnfe-orca-aqj5` | **MSOL/USDC** | `raydium_clmm(GNfe...JKW8)` -> `orca_whirlpool(AqJ5...y9LC)`
- `msol-usdc-orca-aqj5-ray-gnfe` | **MSOL/USDC** | `orca_whirlpool(AqJ5...y9LC)` -> `raydium_clmm(GNfe...JKW8)`
- `pump-usdc-ray-dwga-orca-4afa` | **PUMP/USDC** | `raydium_clmm(Dwga...WA8K)` -> `orca_whirlpool(4AFA...BL2D)`
- `pump-usdc-orca-4afa-ray-dwga` | **PUMP/USDC** | `orca_whirlpool(4AFA...BL2D)` -> `raydium_clmm(Dwga...WA8K)`
- `usdt-sol-ray-3nmf-orca-fwew` | **USDT/SOL** | `raydium_clmm(3nMF...qEgF)` -> `orca_whirlpool(Fwew...qqGC)`
- `usdt-sol-orca-fwew-ray-3nmf` | **USDT/SOL** | `orca_whirlpool(Fwew...qqGC)` -> `raydium_clmm(3nMF...qEgF)`
- `jitosol-sol-ray-2uok-orca-hp53` | **JITOSOL/SOL** | `raydium_clmm(2uoK...L3Mc)` -> `orca_whirlpool(Hp53...XQKp)`
- `jitosol-sol-orca-hp53-ray-2uok` | **JITOSOL/SOL** | `orca_whirlpool(Hp53...XQKp)` -> `raydium_clmm(2uoK...L3Mc)`
- `usd1-sol-ray-aqag-orca-j4jb` | **USD1/SOL** | `raydium_clmm(AQAG...C1FS)` -> `orca_whirlpool(J4jb...4fDV)`
- `usd1-sol-orca-j4jb-ray-aqag` | **USD1/SOL** | `orca_whirlpool(J4jb...4fDV)` -> `raydium_clmm(AQAG...C1FS)`
- `cbbtc-sol-ray-cqxe-orca-ceaz` | **CBBTC/SOL** | `raydium_clmm(CqXe...CrJX)` -> `orca_whirlpool(CeaZ...QpbN)`
- `cbbtc-sol-orca-ceaz-ray-cqxe` | **CBBTC/SOL** | `orca_whirlpool(CeaZ...QpbN)` -> `raydium_clmm(CqXe...CrJX)`
- `pump-sol-ray-45ss-orca-bofa` | **PUMP/SOL** | `raydium_clmm(45ss...uWC5)` -> `orca_whirlpool(BofA...5BKW)`
- `pump-sol-orca-bofa-ray-45ss` | **PUMP/SOL** | `orca_whirlpool(BofA...5BKW)` -> `raydium_clmm(45ss...uWC5)`
- `render-sol-ray-fz8m-orca-amxr` | **RENDER/SOL** | `raydium_clmm(FZ8M...JkNL)` -> `orca_whirlpool(AmXR...w1a3)`
- `render-sol-orca-amxr-ray-fz8m` | **RENDER/SOL** | `orca_whirlpool(AmXR...w1a3)` -> `raydium_clmm(FZ8M...JkNL)`

## `amm_cross_venue_live_trimmed.toml`

Nombre d'entrées: **26**.

- `jlp-sol-ray-8kbr-orca-6a3m` | **JLP/SOL** | `raydium_clmm(8Kbr...4U7Q)` -> `orca_whirlpool(6a3m...xYgd)`
- `jlp-sol-orca-6a3m-ray-8kbr` | **JLP/SOL** | `orca_whirlpool(6a3m...xYgd)` -> `raydium_clmm(8Kbr...4U7Q)`
- `wbtc-sol-ray-hcfy-orca-b5ew` | **WBTC/SOL** | `raydium_clmm(HCfy...KDdA)` -> `orca_whirlpool(B5Ew...4PQA)`
- `wbtc-sol-orca-b5ew-ray-hcfy` | **WBTC/SOL** | `orca_whirlpool(B5Ew...4PQA)` -> `raydium_clmm(HCfy...KDdA)`
- `usdt-usdc-ray-bztg-orca-4fuu` | **USDT/USDC** | `raydium_clmm(BZtg...8mUU)` -> `orca_whirlpool(4fuU...y4T4)`
- `usdt-usdc-orca-4fuu-ray-bztg` | **USDT/USDC** | `orca_whirlpool(4fuU...y4T4)` -> `raydium_clmm(BZtg...8mUU)`
- `eurc-usdc-ray-2zvv-orca-aris` | **EURC/USDC** | `raydium_clmm(2zVV...sK1w)` -> `orca_whirlpool(Aris...oZ9i)`
- `eurc-usdc-orca-aris-ray-2zvv` | **EURC/USDC** | `orca_whirlpool(Aris...oZ9i)` -> `raydium_clmm(2zVV...sK1w)`
- `sol-usdc-ray-3ucn-orca-czfq` | **SOL/USDC** | `raydium_clmm(3ucN...sUxv)` -> `orca_whirlpool(Czfq...44zE)`
- `sol-usdc-orca-czfq-ray-3ucn` | **SOL/USDC** | `orca_whirlpool(Czfq...44zE)` -> `raydium_clmm(3ucN...sUxv)`
- `nvdax-usdc-ray-49im-orca-6r4r` | **NVDAX/USDC** | `raydium_clmm(49iM...yyw6)` -> `orca_whirlpool(6R4r...TXCo)`
- `nvdax-usdc-orca-6r4r-ray-49im` | **NVDAX/USDC** | `orca_whirlpool(6R4r...TXCo)` -> `raydium_clmm(49iM...yyw6)`
- `spyx-usdc-ray-6tru-orca-fae5` | **SPYX/USDC** | `raydium_clmm(6tru...nDDE)` -> `orca_whirlpool(Fae5...CsHR)`
- `spyx-usdc-orca-fae5-ray-6tru` | **SPYX/USDC** | `orca_whirlpool(Fae5...CsHR)` -> `raydium_clmm(6tru...nDDE)`
- `crclx-usdc-ray-gyqh-orca-5thn` | **CRCLX/USDC** | `raydium_clmm(GYqH...yaFV)` -> `orca_whirlpool(5ThN...qsK3)`
- `crclx-usdc-orca-5thn-ray-gyqh` | **CRCLX/USDC** | `orca_whirlpool(5ThN...qsK3)` -> `raydium_clmm(GYqH...yaFV)`
- `msol-usdc-ray-gnfe-orca-aqj5` | **MSOL/USDC** | `raydium_clmm(GNfe...JKW8)` -> `orca_whirlpool(AqJ5...y9LC)`
- `msol-usdc-orca-aqj5-ray-gnfe` | **MSOL/USDC** | `orca_whirlpool(AqJ5...y9LC)` -> `raydium_clmm(GNfe...JKW8)`
- `usdt-sol-ray-3nmf-orca-fwew` | **USDT/SOL** | `raydium_clmm(3nMF...qEgF)` -> `orca_whirlpool(Fwew...qqGC)`
- `usdt-sol-orca-fwew-ray-3nmf` | **USDT/SOL** | `orca_whirlpool(Fwew...qqGC)` -> `raydium_clmm(3nMF...qEgF)`
- `jitosol-sol-ray-2uok-orca-hp53` | **JITOSOL/SOL** | `raydium_clmm(2uoK...L3Mc)` -> `orca_whirlpool(Hp53...XQKp)`
- `jitosol-sol-orca-hp53-ray-2uok` | **JITOSOL/SOL** | `orca_whirlpool(Hp53...XQKp)` -> `raydium_clmm(2uoK...L3Mc)`
- `cbbtc-sol-ray-cqxe-orca-ceaz` | **CBBTC/SOL** | `raydium_clmm(CqXe...CrJX)` -> `orca_whirlpool(CeaZ...QpbN)`
- `cbbtc-sol-orca-ceaz-ray-cqxe` | **CBBTC/SOL** | `orca_whirlpool(CeaZ...QpbN)` -> `raydium_clmm(CqXe...CrJX)`
- `render-sol-ray-fz8m-orca-amxr` | **RENDER/SOL** | `raydium_clmm(FZ8M...JkNL)` -> `orca_whirlpool(AmXR...w1a3)`
- `render-sol-orca-amxr-ray-fz8m` | **RENDER/SOL** | `orca_whirlpool(AmXR...w1a3)` -> `raydium_clmm(FZ8M...JkNL)`

## `amm_cross_venue_plus_intra_ray_fast.toml`

Nombre d'entrées: **68**.

- `jlp-sol-ray-8kbr-orca-6a3m` | **JLP/SOL** | `raydium_clmm(8Kbr...4U7Q)` -> `orca_whirlpool(6a3m...xYgd)`
- `jlp-sol-orca-6a3m-ray-8kbr` | **JLP/SOL** | `orca_whirlpool(6a3m...xYgd)` -> `raydium_clmm(8Kbr...4U7Q)`
- `wbtc-sol-ray-hcfy-orca-b5ew` | **WBTC/SOL** | `raydium_clmm(HCfy...KDdA)` -> `orca_whirlpool(B5Ew...4PQA)`
- `wbtc-sol-orca-b5ew-ray-hcfy` | **WBTC/SOL** | `orca_whirlpool(B5Ew...4PQA)` -> `raydium_clmm(HCfy...KDdA)`
- `usx-usdc-ray-ewiv-orca-2e3w` | **USX/USDC** | `raydium_clmm(EWiv...Prp6)` -> `orca_whirlpool(2e3W...Ybix)`
- `usx-usdc-orca-2e3w-ray-ewiv` | **USX/USDC** | `orca_whirlpool(2e3W...Ybix)` -> `raydium_clmm(EWiv...Prp6)`
- `geod-usdc-ray-14ik-orca-ig9e` | **GEOD/USDC** | `raydium_clmm(14ik...Eey1)` -> `orca_whirlpool(ig9E...6QkC)`
- `geod-usdc-orca-ig9e-ray-14ik` | **GEOD/USDC** | `orca_whirlpool(ig9E...6QkC)` -> `raydium_clmm(14ik...Eey1)`
- `usdt-usdc-ray-bztg-orca-4fuu` | **USDT/USDC** | `raydium_clmm(BZtg...8mUU)` -> `orca_whirlpool(4fuU...y4T4)`
- `usdt-usdc-orca-4fuu-ray-bztg` | **USDT/USDC** | `orca_whirlpool(4fuU...y4T4)` -> `raydium_clmm(BZtg...8mUU)`
- `eurc-usdc-ray-2zvv-orca-aris` | **EURC/USDC** | `raydium_clmm(2zVV...sK1w)` -> `orca_whirlpool(Aris...oZ9i)`
- `eurc-usdc-orca-aris-ray-2zvv` | **EURC/USDC** | `orca_whirlpool(Aris...oZ9i)` -> `raydium_clmm(2zVV...sK1w)`
- `sol-usdc-ray-3ucn-orca-czfq` | **SOL/USDC** | `raydium_clmm(3ucN...sUxv)` -> `orca_whirlpool(Czfq...44zE)`
- `sol-usdc-orca-czfq-ray-3ucn` | **SOL/USDC** | `orca_whirlpool(Czfq...44zE)` -> `raydium_clmm(3ucN...sUxv)`
- `tslax-usdc-ray-8ada-orca-9p7a` | **TSLAX/USDC** | `raydium_clmm(8aDa...NpFF)` -> `orca_whirlpool(9p7a...xokN)`
- `tslax-usdc-orca-9p7a-ray-8ada` | **TSLAX/USDC** | `orca_whirlpool(9p7a...xokN)` -> `raydium_clmm(8aDa...NpFF)`
- `nvdax-usdc-ray-49im-orca-6r4r` | **NVDAX/USDC** | `raydium_clmm(49iM...yyw6)` -> `orca_whirlpool(6R4r...TXCo)`
- `nvdax-usdc-orca-6r4r-ray-49im` | **NVDAX/USDC** | `orca_whirlpool(6R4r...TXCo)` -> `raydium_clmm(49iM...yyw6)`
- `spyx-usdc-ray-6tru-orca-fae5` | **SPYX/USDC** | `raydium_clmm(6tru...nDDE)` -> `orca_whirlpool(Fae5...CsHR)`
- `spyx-usdc-orca-fae5-ray-6tru` | **SPYX/USDC** | `orca_whirlpool(Fae5...CsHR)` -> `raydium_clmm(6tru...nDDE)`
- `crclx-usdc-ray-gyqh-orca-5thn` | **CRCLX/USDC** | `raydium_clmm(GYqH...yaFV)` -> `orca_whirlpool(5ThN...qsK3)`
- `crclx-usdc-orca-5thn-ray-gyqh` | **CRCLX/USDC** | `orca_whirlpool(5ThN...qsK3)` -> `raydium_clmm(GYqH...yaFV)`
- `gldx-usdc-ray-78re-orca-9cru` | **GLDX/USDC** | `raydium_clmm(78Re...hyze)` -> `orca_whirlpool(9crU...gLmx)`
- `gldx-usdc-orca-9cru-ray-78re` | **GLDX/USDC** | `orca_whirlpool(9crU...gLmx)` -> `raydium_clmm(78Re...hyze)`
- `msol-usdc-ray-gnfe-orca-aqj5` | **MSOL/USDC** | `raydium_clmm(GNfe...JKW8)` -> `orca_whirlpool(AqJ5...y9LC)`
- `msol-usdc-orca-aqj5-ray-gnfe` | **MSOL/USDC** | `orca_whirlpool(AqJ5...y9LC)` -> `raydium_clmm(GNfe...JKW8)`
- `pump-usdc-ray-dwga-orca-4afa` | **PUMP/USDC** | `raydium_clmm(Dwga...WA8K)` -> `orca_whirlpool(4AFA...BL2D)`
- `pump-usdc-orca-4afa-ray-dwga` | **PUMP/USDC** | `orca_whirlpool(4AFA...BL2D)` -> `raydium_clmm(Dwga...WA8K)`
- `usdt-sol-ray-3nmf-orca-fwew` | **USDT/SOL** | `raydium_clmm(3nMF...qEgF)` -> `orca_whirlpool(Fwew...qqGC)`
- `usdt-sol-orca-fwew-ray-3nmf` | **USDT/SOL** | `orca_whirlpool(Fwew...qqGC)` -> `raydium_clmm(3nMF...qEgF)`
- `jitosol-sol-ray-2uok-orca-hp53` | **JITOSOL/SOL** | `raydium_clmm(2uoK...L3Mc)` -> `orca_whirlpool(Hp53...XQKp)`
- `jitosol-sol-orca-hp53-ray-2uok` | **JITOSOL/SOL** | `orca_whirlpool(Hp53...XQKp)` -> `raydium_clmm(2uoK...L3Mc)`
- `usd1-sol-ray-aqag-orca-j4jb` | **USD1/SOL** | `raydium_clmm(AQAG...C1FS)` -> `orca_whirlpool(J4jb...4fDV)`
- `usd1-sol-orca-j4jb-ray-aqag` | **USD1/SOL** | `orca_whirlpool(J4jb...4fDV)` -> `raydium_clmm(AQAG...C1FS)`
- `cbbtc-sol-ray-cqxe-orca-ceaz` | **CBBTC/SOL** | `raydium_clmm(CqXe...CrJX)` -> `orca_whirlpool(CeaZ...QpbN)`
- `cbbtc-sol-orca-ceaz-ray-cqxe` | **CBBTC/SOL** | `orca_whirlpool(CeaZ...QpbN)` -> `raydium_clmm(CqXe...CrJX)`
- `pump-sol-ray-45ss-orca-bofa` | **PUMP/SOL** | `raydium_clmm(45ss...uWC5)` -> `orca_whirlpool(BofA...5BKW)`
- `pump-sol-orca-bofa-ray-45ss` | **PUMP/SOL** | `orca_whirlpool(BofA...5BKW)` -> `raydium_clmm(45ss...uWC5)`
- `render-sol-ray-fz8m-orca-amxr` | **RENDER/SOL** | `raydium_clmm(FZ8M...JkNL)` -> `orca_whirlpool(AmXR...w1a3)`
- `render-sol-orca-amxr-ray-fz8m` | **RENDER/SOL** | `orca_whirlpool(AmXR...w1a3)` -> `raydium_clmm(FZ8M...JkNL)`
- `jlp-sol-ray-8kbr-ray-fc3a` | **JLP/SOL** | `raydium_clmm(8Kbr...4U7Q)` -> `raydium_clmm(FC3a...kyQE)`
- `jlp-sol-ray-fc3a-ray-8kbr` | **JLP/SOL** | `raydium_clmm(FC3a...kyQE)` -> `raydium_clmm(8Kbr...4U7Q)`
- `ray-sol-ray-2axx-ray-enfo` | **RAY/SOL** | `raydium_clmm(2AXX...rvY2)` -> `raydium_clmm(Enfo...EBS1)`
- `ray-sol-ray-enfo-ray-2axx` | **RAY/SOL** | `raydium_clmm(Enfo...EBS1)` -> `raydium_clmm(2AXX...rvY2)`
- `geod-usdc-ray-14ik-ray-bkx9` | **GEOD/USDC** | `raydium_clmm(14ik...Eey1)` -> `raydium_clmm(BkX9...GJ5d)`
- `geod-usdc-ray-bkx9-ray-14ik` | **GEOD/USDC** | `raydium_clmm(BkX9...GJ5d)` -> `raydium_clmm(14ik...Eey1)`
- `chatoshi-usdc-ray-hq7y-ray-fi4h` | **CHATOSHI/USDC** | `raydium_clmm(HQ7Y...KaTf)` -> `raydium_clmm(Fi4h...1ADX)`
- `chatoshi-usdc-ray-fi4h-ray-hq7y` | **CHATOSHI/USDC** | `raydium_clmm(Fi4h...1ADX)` -> `raydium_clmm(HQ7Y...KaTf)`
- `sol-usdc-ray-3ucn-ray-cybd` | **SOL/USDC** | `raydium_clmm(3ucN...sUxv)` -> `raydium_clmm(CYbD...tuxq)`
- `sol-usdc-ray-cybd-ray-3ucn` | **SOL/USDC** | `raydium_clmm(CYbD...tuxq)` -> `raydium_clmm(3ucN...sUxv)`
- `qqqx-usdc-ray-gmjg-ray-fknd` | **QQQX/USDC** | `raydium_clmm(GMjG...U1aG)` -> `raydium_clmm(FknD...hpZw)`
- `qqqx-usdc-ray-fknd-ray-gmjg` | **QQQX/USDC** | `raydium_clmm(FknD...hpZw)` -> `raydium_clmm(GMjG...U1aG)`
- `tslax-usdc-ray-8ada-ray-hhqu` | **TSLAX/USDC** | `raydium_clmm(8aDa...NpFF)` -> `raydium_clmm(HHQU...mvum)`
- `tslax-usdc-ray-hhqu-ray-8ada` | **TSLAX/USDC** | `raydium_clmm(HHQU...mvum)` -> `raydium_clmm(8aDa...NpFF)`
- `nvdax-usdc-ray-49im-ray-4kqq` | **NVDAX/USDC** | `raydium_clmm(49iM...yyw6)` -> `raydium_clmm(4KqQ...RBYN)`
- `nvdax-usdc-ray-4kqq-ray-49im` | **NVDAX/USDC** | `raydium_clmm(4KqQ...RBYN)` -> `raydium_clmm(49iM...yyw6)`
- `spyx-usdc-ray-6tru-ray-7shm` | **SPYX/USDC** | `raydium_clmm(6tru...nDDE)` -> `raydium_clmm(7sHM...CqME)`
- `spyx-usdc-ray-7shm-ray-6tru` | **SPYX/USDC** | `raydium_clmm(7sHM...CqME)` -> `raydium_clmm(6tru...nDDE)`
- `crclx-usdc-ray-gyqh-ray-g39w` | **CRCLX/USDC** | `raydium_clmm(GYqH...yaFV)` -> `raydium_clmm(G39w...5axy)`
- `crclx-usdc-ray-g39w-ray-gyqh` | **CRCLX/USDC** | `raydium_clmm(G39w...5axy)` -> `raydium_clmm(GYqH...yaFV)`
- `usdt-sol-ray-3nmf-ray-6kt4` | **USDT/SOL** | `raydium_clmm(3nMF...qEgF)` -> `raydium_clmm(6kT4...YcjS)`
- `usdt-sol-ray-6kt4-ray-3nmf` | **USDT/SOL** | `raydium_clmm(6kT4...YcjS)` -> `raydium_clmm(3nMF...qEgF)`
- `hg8bkz-sol-ray-geac-ray-4dfr` | **HG8BKZ/SOL** | `raydium_clmm(GEac...aLF2)` -> `raydium_clmm(4Dfr...No6a)`
- `hg8bkz-sol-ray-4dfr-ray-geac` | **HG8BKZ/SOL** | `raydium_clmm(4Dfr...No6a)` -> `raydium_clmm(GEac...aLF2)`
- `usd1-sol-ray-aqag-ray-g8lq` | **USD1/SOL** | `raydium_clmm(AQAG...C1FS)` -> `raydium_clmm(G8Lq...rJVr)`
- `usd1-sol-ray-g8lq-ray-aqag` | **USD1/SOL** | `raydium_clmm(G8Lq...rJVr)` -> `raydium_clmm(AQAG...C1FS)`
- `pump-sol-ray-45ss-ray-fkpc` | **PUMP/SOL** | `raydium_clmm(45ss...uWC5)` -> `raydium_clmm(Fkpc...6K2x)`
- `pump-sol-ray-fkpc-ray-45ss` | **PUMP/SOL** | `raydium_clmm(Fkpc...6K2x)` -> `raydium_clmm(45ss...uWC5)`

## `amm_cross_venue_plus_intra_ray_trimmed.toml`

Nombre d'entrées: **44**.

- `jlp-sol-ray-8kbr-orca-6a3m` | **JLP/SOL** | `raydium_clmm(8Kbr...4U7Q)` -> `orca_whirlpool(6a3m...xYgd)`
- `jlp-sol-orca-6a3m-ray-8kbr` | **JLP/SOL** | `orca_whirlpool(6a3m...xYgd)` -> `raydium_clmm(8Kbr...4U7Q)`
- `wbtc-sol-ray-hcfy-orca-b5ew` | **WBTC/SOL** | `raydium_clmm(HCfy...KDdA)` -> `orca_whirlpool(B5Ew...4PQA)`
- `wbtc-sol-orca-b5ew-ray-hcfy` | **WBTC/SOL** | `orca_whirlpool(B5Ew...4PQA)` -> `raydium_clmm(HCfy...KDdA)`
- `usdt-usdc-ray-bztg-orca-4fuu` | **USDT/USDC** | `raydium_clmm(BZtg...8mUU)` -> `orca_whirlpool(4fuU...y4T4)`
- `usdt-usdc-orca-4fuu-ray-bztg` | **USDT/USDC** | `orca_whirlpool(4fuU...y4T4)` -> `raydium_clmm(BZtg...8mUU)`
- `eurc-usdc-ray-2zvv-orca-aris` | **EURC/USDC** | `raydium_clmm(2zVV...sK1w)` -> `orca_whirlpool(Aris...oZ9i)`
- `eurc-usdc-orca-aris-ray-2zvv` | **EURC/USDC** | `orca_whirlpool(Aris...oZ9i)` -> `raydium_clmm(2zVV...sK1w)`
- `sol-usdc-ray-3ucn-orca-czfq` | **SOL/USDC** | `raydium_clmm(3ucN...sUxv)` -> `orca_whirlpool(Czfq...44zE)`
- `sol-usdc-orca-czfq-ray-3ucn` | **SOL/USDC** | `orca_whirlpool(Czfq...44zE)` -> `raydium_clmm(3ucN...sUxv)`
- `nvdax-usdc-ray-49im-orca-6r4r` | **NVDAX/USDC** | `raydium_clmm(49iM...yyw6)` -> `orca_whirlpool(6R4r...TXCo)`
- `nvdax-usdc-orca-6r4r-ray-49im` | **NVDAX/USDC** | `orca_whirlpool(6R4r...TXCo)` -> `raydium_clmm(49iM...yyw6)`
- `spyx-usdc-ray-6tru-orca-fae5` | **SPYX/USDC** | `raydium_clmm(6tru...nDDE)` -> `orca_whirlpool(Fae5...CsHR)`
- `spyx-usdc-orca-fae5-ray-6tru` | **SPYX/USDC** | `orca_whirlpool(Fae5...CsHR)` -> `raydium_clmm(6tru...nDDE)`
- `crclx-usdc-ray-gyqh-orca-5thn` | **CRCLX/USDC** | `raydium_clmm(GYqH...yaFV)` -> `orca_whirlpool(5ThN...qsK3)`
- `crclx-usdc-orca-5thn-ray-gyqh` | **CRCLX/USDC** | `orca_whirlpool(5ThN...qsK3)` -> `raydium_clmm(GYqH...yaFV)`
- `msol-usdc-ray-gnfe-orca-aqj5` | **MSOL/USDC** | `raydium_clmm(GNfe...JKW8)` -> `orca_whirlpool(AqJ5...y9LC)`
- `msol-usdc-orca-aqj5-ray-gnfe` | **MSOL/USDC** | `orca_whirlpool(AqJ5...y9LC)` -> `raydium_clmm(GNfe...JKW8)`
- `jitosol-sol-ray-2uok-orca-hp53` | **JITOSOL/SOL** | `raydium_clmm(2uoK...L3Mc)` -> `orca_whirlpool(Hp53...XQKp)`
- `jitosol-sol-orca-hp53-ray-2uok` | **JITOSOL/SOL** | `orca_whirlpool(Hp53...XQKp)` -> `raydium_clmm(2uoK...L3Mc)`
- `cbbtc-sol-ray-cqxe-orca-ceaz` | **CBBTC/SOL** | `raydium_clmm(CqXe...CrJX)` -> `orca_whirlpool(CeaZ...QpbN)`
- `cbbtc-sol-orca-ceaz-ray-cqxe` | **CBBTC/SOL** | `orca_whirlpool(CeaZ...QpbN)` -> `raydium_clmm(CqXe...CrJX)`
- `render-sol-ray-fz8m-orca-amxr` | **RENDER/SOL** | `raydium_clmm(FZ8M...JkNL)` -> `orca_whirlpool(AmXR...w1a3)`
- `render-sol-orca-amxr-ray-fz8m` | **RENDER/SOL** | `orca_whirlpool(AmXR...w1a3)` -> `raydium_clmm(FZ8M...JkNL)`
- `ray-sol-ray-2axx-ray-enfo` | **RAY/SOL** | `raydium_clmm(2AXX...rvY2)` -> `raydium_clmm(Enfo...EBS1)`
- `ray-sol-ray-enfo-ray-2axx` | **RAY/SOL** | `raydium_clmm(Enfo...EBS1)` -> `raydium_clmm(2AXX...rvY2)`
- `geod-usdc-ray-14ik-ray-bkx9` | **GEOD/USDC** | `raydium_clmm(14ik...Eey1)` -> `raydium_clmm(BkX9...GJ5d)`
- `geod-usdc-ray-bkx9-ray-14ik` | **GEOD/USDC** | `raydium_clmm(BkX9...GJ5d)` -> `raydium_clmm(14ik...Eey1)`
- `qqqx-usdc-ray-gmjg-ray-fknd` | **QQQX/USDC** | `raydium_clmm(GMjG...U1aG)` -> `raydium_clmm(FknD...hpZw)`
- `qqqx-usdc-ray-fknd-ray-gmjg` | **QQQX/USDC** | `raydium_clmm(FknD...hpZw)` -> `raydium_clmm(GMjG...U1aG)`
- `tslax-usdc-ray-8ada-ray-hhqu` | **TSLAX/USDC** | `raydium_clmm(8aDa...NpFF)` -> `raydium_clmm(HHQU...mvum)`
- `tslax-usdc-ray-hhqu-ray-8ada` | **TSLAX/USDC** | `raydium_clmm(HHQU...mvum)` -> `raydium_clmm(8aDa...NpFF)`
- `nvdax-usdc-ray-49im-ray-4kqq` | **NVDAX/USDC** | `raydium_clmm(49iM...yyw6)` -> `raydium_clmm(4KqQ...RBYN)`
- `nvdax-usdc-ray-4kqq-ray-49im` | **NVDAX/USDC** | `raydium_clmm(4KqQ...RBYN)` -> `raydium_clmm(49iM...yyw6)`
- `spyx-usdc-ray-6tru-ray-7shm` | **SPYX/USDC** | `raydium_clmm(6tru...nDDE)` -> `raydium_clmm(7sHM...CqME)`
- `spyx-usdc-ray-7shm-ray-6tru` | **SPYX/USDC** | `raydium_clmm(7sHM...CqME)` -> `raydium_clmm(6tru...nDDE)`
- `crclx-usdc-ray-gyqh-ray-g39w` | **CRCLX/USDC** | `raydium_clmm(GYqH...yaFV)` -> `raydium_clmm(G39w...5axy)`
- `crclx-usdc-ray-g39w-ray-gyqh` | **CRCLX/USDC** | `raydium_clmm(G39w...5axy)` -> `raydium_clmm(GYqH...yaFV)`
- `hg8bkz-sol-ray-geac-ray-4dfr` | **HG8BKZ/SOL** | `raydium_clmm(GEac...aLF2)` -> `raydium_clmm(4Dfr...No6a)`
- `hg8bkz-sol-ray-4dfr-ray-geac` | **HG8BKZ/SOL** | `raydium_clmm(4Dfr...No6a)` -> `raydium_clmm(GEac...aLF2)`
- `usd1-sol-ray-aqag-ray-g8lq` | **USD1/SOL** | `raydium_clmm(AQAG...C1FS)` -> `raydium_clmm(G8Lq...rJVr)`
- `usd1-sol-ray-g8lq-ray-aqag` | **USD1/SOL** | `raydium_clmm(G8Lq...rJVr)` -> `raydium_clmm(AQAG...C1FS)`
- `pump-sol-ray-45ss-ray-fkpc` | **PUMP/SOL** | `raydium_clmm(45ss...uWC5)` -> `raydium_clmm(Fkpc...6K2x)`
- `pump-sol-ray-fkpc-ray-45ss` | **PUMP/SOL** | `raydium_clmm(Fkpc...6K2x)` -> `raydium_clmm(45ss...uWC5)`

## `amm_cross_venue_plus_simple_fast.toml`

Nombre d'entrées: **96**.

- `jlp-sol-orca-6a3m-ray-8kbr` | **JLP/SOL** | `orca_whirlpool(6a3m...xYgd)` -> `raydium_clmm(8Kbr...4U7Q)`
- `jlp-sol-ray-8kbr-orca-6a3m` | **JLP/SOL** | `raydium_clmm(8Kbr...4U7Q)` -> `orca_whirlpool(6a3m...xYgd)`
- `wbtc-sol-orca-b5ew-ray-hcfy` | **WBTC/SOL** | `orca_whirlpool(B5Ew...4PQA)` -> `raydium_clmm(HCfy...KDdA)`
- `wbtc-sol-ray-hcfy-orca-b5ew` | **WBTC/SOL** | `raydium_clmm(HCfy...KDdA)` -> `orca_whirlpool(B5Ew...4PQA)`
- `ray-usdc-ray-61r1-ray-6umm` | **RAY/USDC** | `raydium_clmm(61R1...C8ht)` -> `raydium(6Umm...o1mg)`
- `ray-usdc-ray-6umm-ray-61r1` | **RAY/USDC** | `raydium(6Umm...o1mg)` -> `raydium_clmm(61R1...C8ht)`
- `usx-usdc-orca-2e3w-ray-ewiv` | **USX/USDC** | `orca_whirlpool(2e3W...Ybix)` -> `raydium_clmm(EWiv...Prp6)`
- `usx-usdc-ray-ewiv-orca-2e3w` | **USX/USDC** | `raydium_clmm(EWiv...Prp6)` -> `orca_whirlpool(2e3W...Ybix)`
- `geod-usdc-orca-ig9e-ray-14ik` | **GEOD/USDC** | `orca_whirlpool(ig9E...6QkC)` -> `raydium_clmm(14ik...Eey1)`
- `geod-usdc-ray-14ik-orca-ig9e` | **GEOD/USDC** | `raydium_clmm(14ik...Eey1)` -> `orca_whirlpool(ig9E...6QkC)`
- `fartcoin-sol-orca-c9u2-ray-bzc9` | **FARTCOIN/SOL** | `orca_whirlpool(C9U2...aUSM)` -> `raydium(Bzc9...5iiw)`
- `fartcoin-sol-ray-bzc9-orca-c9u2` | **FARTCOIN/SOL** | `raydium(Bzc9...5iiw)` -> `orca_whirlpool(C9U2...aUSM)`
- `wif-sol-orca-d6nd-ray-ep2i` | **WIF/SOL** | `orca_whirlpool(D6Nd...9Qz1)` -> `raydium(EP2i...eyMx)`
- `wif-sol-ray-ep2i-orca-d6nd` | **WIF/SOL** | `raydium(EP2i...eyMx)` -> `orca_whirlpool(D6Nd...9Qz1)`
- `usdt-usdc-orca-4fuu-ray-bztg` | **USDT/USDC** | `orca_whirlpool(4fuU...y4T4)` -> `raydium_clmm(BZtg...8mUU)`
- `usdt-usdc-orca-4fuu-ray-7tbg` | **USDT/USDC** | `orca_whirlpool(4fuU...y4T4)` -> `raydium(7TbG...woyF)`
- `usdt-usdc-ray-bztg-orca-4fuu` | **USDT/USDC** | `raydium_clmm(BZtg...8mUU)` -> `orca_whirlpool(4fuU...y4T4)`
- `usdt-usdc-ray-bztg-ray-7tbg` | **USDT/USDC** | `raydium_clmm(BZtg...8mUU)` -> `raydium(7TbG...woyF)`
- `usdt-usdc-ray-7tbg-orca-4fuu` | **USDT/USDC** | `raydium(7TbG...woyF)` -> `orca_whirlpool(4fuU...y4T4)`
- `usdt-usdc-ray-7tbg-ray-bztg` | **USDT/USDC** | `raydium(7TbG...woyF)` -> `raydium_clmm(BZtg...8mUU)`
- `eurc-usdc-orca-aris-ray-2zvv` | **EURC/USDC** | `orca_whirlpool(Aris...oZ9i)` -> `raydium_clmm(2zVV...sK1w)`
- `eurc-usdc-ray-2zvv-orca-aris` | **EURC/USDC** | `raydium_clmm(2zVV...sK1w)` -> `orca_whirlpool(Aris...oZ9i)`
- `sol-usdc-orca-czfq-ray-3ucn` | **SOL/USDC** | `orca_whirlpool(Czfq...44zE)` -> `raydium_clmm(3ucN...sUxv)`
- `sol-usdc-orca-czfq-ray-58oq` | **SOL/USDC** | `orca_whirlpool(Czfq...44zE)` -> `raydium(58oQ...YQo2)`
- `sol-usdc-ray-3ucn-orca-czfq` | **SOL/USDC** | `raydium_clmm(3ucN...sUxv)` -> `orca_whirlpool(Czfq...44zE)`
- `sol-usdc-ray-3ucn-ray-58oq` | **SOL/USDC** | `raydium_clmm(3ucN...sUxv)` -> `raydium(58oQ...YQo2)`
- `sol-usdc-ray-58oq-orca-czfq` | **SOL/USDC** | `raydium(58oQ...YQo2)` -> `orca_whirlpool(Czfq...44zE)`
- `sol-usdc-ray-58oq-ray-3ucn` | **SOL/USDC** | `raydium(58oQ...YQo2)` -> `raydium_clmm(3ucN...sUxv)`
- `tslax-usdc-orca-9p7a-ray-8ada` | **TSLAX/USDC** | `orca_whirlpool(9p7a...xokN)` -> `raydium_clmm(8aDa...NpFF)`
- `tslax-usdc-ray-8ada-orca-9p7a` | **TSLAX/USDC** | `raydium_clmm(8aDa...NpFF)` -> `orca_whirlpool(9p7a...xokN)`
- `nvdax-usdc-orca-6r4r-ray-49im` | **NVDAX/USDC** | `orca_whirlpool(6R4r...TXCo)` -> `raydium_clmm(49iM...yyw6)`
- `nvdax-usdc-ray-49im-orca-6r4r` | **NVDAX/USDC** | `raydium_clmm(49iM...yyw6)` -> `orca_whirlpool(6R4r...TXCo)`
- `spyx-usdc-orca-fae5-ray-6tru` | **SPYX/USDC** | `orca_whirlpool(Fae5...CsHR)` -> `raydium_clmm(6tru...nDDE)`
- `spyx-usdc-ray-6tru-orca-fae5` | **SPYX/USDC** | `raydium_clmm(6tru...nDDE)` -> `orca_whirlpool(Fae5...CsHR)`
- `gldx-usdc-orca-9cru-ray-78re` | **GLDX/USDC** | `orca_whirlpool(9crU...gLmx)` -> `raydium_clmm(78Re...hyze)`
- `gldx-usdc-ray-78re-orca-9cru` | **GLDX/USDC** | `raydium_clmm(78Re...hyze)` -> `orca_whirlpool(9crU...gLmx)`
- `msol-usdc-orca-aqj5-ray-gnfe` | **MSOL/USDC** | `orca_whirlpool(AqJ5...y9LC)` -> `raydium_clmm(GNfe...JKW8)`
- `msol-usdc-orca-aqj5-ray-zfvd` | **MSOL/USDC** | `orca_whirlpool(AqJ5...y9LC)` -> `raydium(ZfvD...Z9ix)`
- `msol-usdc-ray-gnfe-orca-aqj5` | **MSOL/USDC** | `raydium_clmm(GNfe...JKW8)` -> `orca_whirlpool(AqJ5...y9LC)`
- `msol-usdc-ray-gnfe-ray-zfvd` | **MSOL/USDC** | `raydium_clmm(GNfe...JKW8)` -> `raydium(ZfvD...Z9ix)`
- `msol-usdc-ray-zfvd-orca-aqj5` | **MSOL/USDC** | `raydium(ZfvD...Z9ix)` -> `orca_whirlpool(AqJ5...y9LC)`
- `msol-usdc-ray-zfvd-ray-gnfe` | **MSOL/USDC** | `raydium(ZfvD...Z9ix)` -> `raydium_clmm(GNfe...JKW8)`
- `pump-usdc-orca-4afa-ray-dwga` | **PUMP/USDC** | `orca_whirlpool(4AFA...BL2D)` -> `raydium_clmm(Dwga...WA8K)`
- `pump-usdc-ray-dwga-orca-4afa` | **PUMP/USDC** | `raydium_clmm(Dwga...WA8K)` -> `orca_whirlpool(4AFA...BL2D)`
- `usdt-sol-orca-fwew-ray-3nmf` | **USDT/SOL** | `orca_whirlpool(Fwew...qqGC)` -> `raydium_clmm(3nMF...qEgF)`
- `usdt-sol-orca-fwew-ray-7xaw` | **USDT/SOL** | `orca_whirlpool(Fwew...qqGC)` -> `raydium(7Xaw...bEmX)`
- `usdt-sol-ray-3nmf-orca-fwew` | **USDT/SOL** | `raydium_clmm(3nMF...qEgF)` -> `orca_whirlpool(Fwew...qqGC)`
- `usdt-sol-ray-3nmf-ray-7xaw` | **USDT/SOL** | `raydium_clmm(3nMF...qEgF)` -> `raydium(7Xaw...bEmX)`
- `usdt-sol-ray-7xaw-orca-fwew` | **USDT/SOL** | `raydium(7Xaw...bEmX)` -> `orca_whirlpool(Fwew...qqGC)`
- `usdt-sol-ray-7xaw-ray-3nmf` | **USDT/SOL** | `raydium(7Xaw...bEmX)` -> `raydium_clmm(3nMF...qEgF)`
- `dreams-sol-ray-bwfe-ray-hmqj` | **DREAMS/SOL** | `raydium_clmm(BWfe...TizC)` -> `raydium(HMqJ...LaJQ)`
- `dreams-sol-ray-hmqj-ray-bwfe` | **DREAMS/SOL** | `raydium(HMqJ...LaJQ)` -> `raydium_clmm(BWfe...TizC)`
- `jitosol-sol-orca-hp53-ray-2uok` | **JITOSOL/SOL** | `orca_whirlpool(Hp53...XQKp)` -> `raydium_clmm(2uoK...L3Mc)`
- `jitosol-sol-ray-2uok-orca-hp53` | **JITOSOL/SOL** | `raydium_clmm(2uoK...L3Mc)` -> `orca_whirlpool(Hp53...XQKp)`
- `usd1-sol-orca-j4jb-ray-aqag` | **USD1/SOL** | `orca_whirlpool(J4jb...4fDV)` -> `raydium_clmm(AQAG...C1FS)`
- `usd1-sol-orca-j4jb-ray-fado` | **USD1/SOL** | `orca_whirlpool(J4jb...4fDV)` -> `raydium(FaDo...BkPq)`
- `usd1-sol-ray-aqag-orca-j4jb` | **USD1/SOL** | `raydium_clmm(AQAG...C1FS)` -> `orca_whirlpool(J4jb...4fDV)`
- `usd1-sol-ray-aqag-ray-fado` | **USD1/SOL** | `raydium_clmm(AQAG...C1FS)` -> `raydium(FaDo...BkPq)`
- `usd1-sol-ray-fado-orca-j4jb` | **USD1/SOL** | `raydium(FaDo...BkPq)` -> `orca_whirlpool(J4jb...4fDV)`
- `usd1-sol-ray-fado-ray-aqag` | **USD1/SOL** | `raydium(FaDo...BkPq)` -> `raydium_clmm(AQAG...C1FS)`
- `cbbtc-sol-orca-ceaz-ray-cqxe` | **CBBTC/SOL** | `orca_whirlpool(CeaZ...QpbN)` -> `raydium_clmm(CqXe...CrJX)`
- `cbbtc-sol-ray-cqxe-orca-ceaz` | **CBBTC/SOL** | `raydium_clmm(CqXe...CrJX)` -> `orca_whirlpool(CeaZ...QpbN)`
- `hnt-sol-orca-csp4-ray-91ax` | **HNT/SOL** | `orca_whirlpool(CSP4...qjE4)` -> `raydium(91ax...2JpM)`
- `hnt-sol-ray-91ax-orca-csp4` | **HNT/SOL** | `raydium(91ax...2JpM)` -> `orca_whirlpool(CSP4...qjE4)`
- `pump-sol-orca-bofa-ray-45ss` | **PUMP/SOL** | `orca_whirlpool(BofA...5BKW)` -> `raydium_clmm(45ss...uWC5)`
- `pump-sol-ray-45ss-orca-bofa` | **PUMP/SOL** | `raydium_clmm(45ss...uWC5)` -> `orca_whirlpool(BofA...5BKW)`
- `render-sol-orca-amxr-ray-fz8m` | **RENDER/SOL** | `orca_whirlpool(AmXR...w1a3)` -> `raydium_clmm(FZ8M...JkNL)`
- `render-sol-ray-fz8m-orca-amxr` | **RENDER/SOL** | `raydium_clmm(FZ8M...JkNL)` -> `orca_whirlpool(AmXR...w1a3)`
- `jlp-sol-ray-8kbr-ray-fc3a` | **JLP/SOL** | `raydium_clmm(8Kbr...4U7Q)` -> `raydium_clmm(FC3a...kyQE)`
- `jlp-sol-ray-fc3a-ray-8kbr` | **JLP/SOL** | `raydium_clmm(FC3a...kyQE)` -> `raydium_clmm(8Kbr...4U7Q)`
- `ray-sol-ray-2axx-ray-enfo` | **RAY/SOL** | `raydium_clmm(2AXX...rvY2)` -> `raydium_clmm(Enfo...EBS1)`
- `ray-sol-ray-enfo-ray-2axx` | **RAY/SOL** | `raydium_clmm(Enfo...EBS1)` -> `raydium_clmm(2AXX...rvY2)`
- `geod-usdc-ray-14ik-ray-bkx9` | **GEOD/USDC** | `raydium_clmm(14ik...Eey1)` -> `raydium_clmm(BkX9...GJ5d)`
- `geod-usdc-ray-bkx9-ray-14ik` | **GEOD/USDC** | `raydium_clmm(BkX9...GJ5d)` -> `raydium_clmm(14ik...Eey1)`
- `chatoshi-usdc-ray-hq7y-ray-fi4h` | **CHATOSHI/USDC** | `raydium_clmm(HQ7Y...KaTf)` -> `raydium_clmm(Fi4h...1ADX)`
- `chatoshi-usdc-ray-fi4h-ray-hq7y` | **CHATOSHI/USDC** | `raydium_clmm(Fi4h...1ADX)` -> `raydium_clmm(HQ7Y...KaTf)`
- `sol-usdc-ray-3ucn-ray-cybd` | **SOL/USDC** | `raydium_clmm(3ucN...sUxv)` -> `raydium_clmm(CYbD...tuxq)`
- `sol-usdc-ray-cybd-ray-3ucn` | **SOL/USDC** | `raydium_clmm(CYbD...tuxq)` -> `raydium_clmm(3ucN...sUxv)`
- `qqqx-usdc-ray-gmjg-ray-fknd` | **QQQX/USDC** | `raydium_clmm(GMjG...U1aG)` -> `raydium_clmm(FknD...hpZw)`
- `qqqx-usdc-ray-fknd-ray-gmjg` | **QQQX/USDC** | `raydium_clmm(FknD...hpZw)` -> `raydium_clmm(GMjG...U1aG)`
- `tslax-usdc-ray-8ada-ray-hhqu` | **TSLAX/USDC** | `raydium_clmm(8aDa...NpFF)` -> `raydium_clmm(HHQU...mvum)`
- `tslax-usdc-ray-hhqu-ray-8ada` | **TSLAX/USDC** | `raydium_clmm(HHQU...mvum)` -> `raydium_clmm(8aDa...NpFF)`
- `nvdax-usdc-ray-49im-ray-4kqq` | **NVDAX/USDC** | `raydium_clmm(49iM...yyw6)` -> `raydium_clmm(4KqQ...RBYN)`
- `nvdax-usdc-ray-4kqq-ray-49im` | **NVDAX/USDC** | `raydium_clmm(4KqQ...RBYN)` -> `raydium_clmm(49iM...yyw6)`
- `spyx-usdc-ray-6tru-ray-7shm` | **SPYX/USDC** | `raydium_clmm(6tru...nDDE)` -> `raydium_clmm(7sHM...CqME)`
- `spyx-usdc-ray-7shm-ray-6tru` | **SPYX/USDC** | `raydium_clmm(7sHM...CqME)` -> `raydium_clmm(6tru...nDDE)`
- `crclx-usdc-ray-gyqh-ray-g39w` | **CRCLX/USDC** | `raydium_clmm(GYqH...yaFV)` -> `raydium_clmm(G39w...5axy)`
- `crclx-usdc-ray-g39w-ray-gyqh` | **CRCLX/USDC** | `raydium_clmm(G39w...5axy)` -> `raydium_clmm(GYqH...yaFV)`
- `usdt-sol-ray-3nmf-ray-6kt4` | **USDT/SOL** | `raydium_clmm(3nMF...qEgF)` -> `raydium_clmm(6kT4...YcjS)`
- `usdt-sol-ray-6kt4-ray-3nmf` | **USDT/SOL** | `raydium_clmm(6kT4...YcjS)` -> `raydium_clmm(3nMF...qEgF)`
- `hg8bkz-sol-ray-geac-ray-4dfr` | **HG8BKZ/SOL** | `raydium_clmm(GEac...aLF2)` -> `raydium_clmm(4Dfr...No6a)`
- `hg8bkz-sol-ray-4dfr-ray-geac` | **HG8BKZ/SOL** | `raydium_clmm(4Dfr...No6a)` -> `raydium_clmm(GEac...aLF2)`
- `usd1-sol-ray-aqag-ray-g8lq` | **USD1/SOL** | `raydium_clmm(AQAG...C1FS)` -> `raydium_clmm(G8Lq...rJVr)`
- `usd1-sol-ray-g8lq-ray-aqag` | **USD1/SOL** | `raydium_clmm(G8Lq...rJVr)` -> `raydium_clmm(AQAG...C1FS)`
- `pump-sol-ray-45ss-ray-fkpc` | **PUMP/SOL** | `raydium_clmm(45ss...uWC5)` -> `raydium_clmm(Fkpc...6K2x)`
- `pump-sol-ray-fkpc-ray-45ss` | **PUMP/SOL** | `raydium_clmm(Fkpc...6K2x)` -> `raydium_clmm(45ss...uWC5)`

## `sol_usdc_routes.toml`

Nombre d'entrées: **4**.

- `sol-usdc-orca004-ray004` | **SOL/USDC** | `orca_whirlpool(Czfq...44zE)` -> `raydium_clmm(3ucN...sUxv)`
- `sol-usdc-ray004-orca004` | **SOL/USDC** | `raydium_clmm(3ucN...sUxv)` -> `orca_whirlpool(Czfq...44zE)`
- `sol-usdc-orca004-ray002` | **SOL/USDC** | `orca_whirlpool(Czfq...44zE)` -> `raydium_clmm(CYbD...tuxq)`
- `sol-usdc-ray002-orca004` | **SOL/USDC** | `raydium_clmm(CYbD...tuxq)` -> `orca_whirlpool(Czfq...44zE)`

## `sol_usdc_routes_amm_fast.toml`

Nombre d'entrées: **8**.

- `sol-usdc-ray-61ac-ray-5oav` | **SOL/USDC** | `raydium(61ac...4KLu)` -> `raydium(5oAv...uc6n)`
- `sol-usdc-ray-5oav-ray-61ac` | **SOL/USDC** | `raydium(5oAv...uc6n)` -> `raydium(61ac...4KLu)`
- `jto-sol-ray-jvop-orca-2uhf` | **JTO/SOL** | `raydium_clmm(JVoP...uvZo)` -> `orca_whirlpool(2UhF...4gHJ)`
- `jto-sol-orca-2uhf-ray-jvop` | **JTO/SOL** | `orca_whirlpool(2UhF...4gHJ)` -> `raydium_clmm(JVoP...uvZo)`
- `trump-sol-orca-ckp1-ray-gqsp` | **TRUMP/SOL** | `orca_whirlpool(Ckp1...n7p2)` -> `raydium_clmm(GQsP...vNvW)`
- `trump-sol-ray-gqsp-orca-ckp1` | **TRUMP/SOL** | `raydium_clmm(GQsP...vNvW)` -> `orca_whirlpool(Ckp1...n7p2)`
- `trump-usdc-orca-6nd6-ray-7xzv` | **TRUMP/USDC** | `orca_whirlpool(6nD6...XigJ)` -> `raydium_clmm(7XzV...9kuh)`
- `trump-usdc-ray-7xzv-orca-6nd6` | **TRUMP/USDC** | `raydium_clmm(7XzV...9kuh)` -> `orca_whirlpool(6nD6...XigJ)`
