USE CMS_PDS_DATA;
GO

CREATE TABLE provider_specific_file (
    providerCcn VARCHAR(13),

    -- Converted to DATE
    effectiveDate DATE,
    fiscalYearBeginDate DATE,
    exportDate DATE,
    terminationDate DATE,

    waiverIndicator CHAR(1),
    intermediaryNumber VARCHAR(10),
    providerType VARCHAR(5),
    censusDivision VARCHAR(5),

    msaActualGeographicLocation VARCHAR(10),
    msaWageIndexLocation VARCHAR(10),
    msaStandardizedAmountLocation VARCHAR(10),

    soleCommunityOrMedicareDependentHospitalBaseYear VARCHAR(4),
    changeCodeForLugarReclassification CHAR(1),
    temporaryReliefIndicator CHAR(1),
    federalPpsBlend VARCHAR(5),

    stateCode VARCHAR(5),

    caseMixAdjustedCostPerDischarge_PpsFacilitySpecificRate DECIMAL(18,4),
    costOfLivingAdjustment DECIMAL(18,4),
    internsToBedsRatio DECIMAL(18,4),
    bedSize INT,
    operatingCostToChargeRatio DECIMAL(18,4),
    caseMixIndex DECIMAL(18,4),
    supplementalSecurityIncomeRatio DECIMAL(18,4),
    medicaidRatio DECIMAL(18,4),
    specialProviderUpdateFactor DECIMAL(18,4),
    operatingDsh DECIMAL(18,4),

    -- Converted to DATE
    fiscalYearEndDate DATE,

    specialPaymentIndicator VARCHAR(5),
    hospitalQualityIndicator VARCHAR(5),

    cbsaActualGeographicLocation VARCHAR(10),
    cbsaWageIndexLocation VARCHAR(10),
    cbsaStandardizedAmountLocation VARCHAR(10),

    specialWageIndex DECIMAL(18,4),

    passThroughAmountForCapital DECIMAL(18,4),
    passThroughAmountForDirectMedicalEducation DECIMAL(18,4),
    passThroughAmountForOrganAcquisition DECIMAL(18,4),
    passThroughTotalAmount DECIMAL(18,4),

    capitalPpsPaymentCode CHAR(1),
    hospitalSpecificCapitalRate DECIMAL(18,4),
    oldCapitalHoldHarmlessRate DECIMAL(18,4),
    newCapitalHoldHarmlessRate DECIMAL(18,4),
    capitalCostToChargeRatio DECIMAL(18,4),

    newHospital CHAR(1),
    capitalIndirectMedicalEducationRatio DECIMAL(18,4),
    capitalExceptionPaymentRate DECIMAL(18,4),

    vbpParticipantIndicator CHAR(1),
    vbpAdjustment DECIMAL(18,9),

    hrrParticipantIndicator VARCHAR(5),
    hrrAdjustment DECIMAL(18,9),

    bundleModel1Discount DECIMAL(18,4),
    hacReductionParticipantIndicator CHAR(1),
    uncompensatedCareAmount DECIMAL(18,4),
    ehrReductionIndicator CHAR(1),
    lowVolumeAdjustmentFactor DECIMAL(18,4),

    countyCode VARCHAR(5),
    medicarePerformanceAdjustment DECIMAL(18,9),

    ltchDppIndicator CHAR(1),
    supplementalWageIndex DECIMAL(18,4),
    supplementalWageIndexIndicator VARCHAR(5),
    changeCodeWageIndexReclassification CHAR(1),

    nationalProviderIdentifier VARCHAR(10),

    passThroughAmountForAllogenicStemCellAcquisition DECIMAL(18,4),
    ppsBlendYearIndicator VARCHAR(4),

    -- Converted to DATE
    lastUpdated DATE,

    passThroughAmountForDirectGraduateMedicalEducation DECIMAL(18,4),
    passThroughAmountForKidneyAcquisition DECIMAL(18,4),
    passThroughAmountForSupplyChain DECIMAL(18,4),

    load_timestamp DATETIME DEFAULT GETDATE()
);
GO