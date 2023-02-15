const { getLiftGate, getUnNum } = require("./dataHelper");
const moment = require("moment");
/**
 * non console p2p // send housebill no
 */
const loadP2PNonConsol = (dataSet, shipmentAparData) => {
  console.log("loadP2PNonConsol");
  const confirmationCost = dataSet.confirmationCost;
  const shipmentApar = shipmentAparData;
  const shipmentHeader = dataSet.shipmentHeader;

  const shipmentDesc = dataSet.shipmentDesc.filter((e) => e.ConsolNo === "0");

  const filteredConfirmationCost = confirmationCost.filter((e) => {
    return (
      e.FK_OrderNo === shipmentApar.FK_OrderNo &&
      e.FK_SeqNo === shipmentApar.SeqNo &&
      shipmentApar.Consolidation === "N"
    );
  });

  const housebill_delimited = shipmentHeader.map((e) => e.Housebill);

  const cargo = shipmentDesc.map((e) => {
    return {
      packageType:
        e.FK_PieceTypeId === "BOX"
          ? "BOX"
          : e.FK_PieceTypeId === "PLT"
          ? "PAL"
          : "PIE",
      quantity: e?.Pieces ?? "",
      length: e?.Length ? parseInt(e?.Length) : "",
      width: e?.Width ? parseInt(e?.Width) : "",
      height: e?.Height ? parseInt(e?.Height) : "",
      weight: e?.Weight ? parseInt(e?.Weight) : "",
      stackable: "Y", // hardcode
      turnable: "Y", // hardcode
    };
  });

  const pStopTypeData = JSON.parse(
    JSON.stringify(filteredConfirmationCost)
  ).map((e) => {
    return {
      stopType: "P",
      stopNum: 0,
      housebills: housebill_delimited,
      address: {
        address1: e.ShipAddress1,
        city: e.ShipCity,
        country: e.FK_ShipCountry,
        state: e.FK_ShipState,
        zip: e.ShipZip,
      },
      companyName: e.ShipName,
      cargo: cargo,
      scheduledDate: moment(e.PickupDateTime).diff("1970-01-01", "ms"), // ??
      specialInstructions:
        e.ShipAddress2 === "" ? "" : e.ShipAddress2 + " " + e.PickupNote,
    };
  });

  const dStopTypeData = JSON.parse(
    JSON.stringify(filteredConfirmationCost)
  ).map((e) => {
    return {
      stopType: "D",
      stopNum: 1,
      housebills: housebill_delimited,
      address: {
        address1: e.ConAddress1,
        city: e.ConCity,
        country: e.FK_ConCountry,
        state: e.FK_ConState,
        zip: e.ConZip,
      },
      companyName: e.ConName,
      scheduledDate: moment(e.DeliveryDateTime).diff("1970-01-01", "ms"), // ??
      specialInstructions:
        e.ConAddress2 === "" ? "" : e.ConAddress2 + " " + e.PickupNote,
    };
  });

  const iviaPayload = {
    carrierId: process.env.IVIA_CARRIER_ID, // process.env.IVIA_CARRIER_ID = 1000025
    refNums: {
      refNum1: dataSet.shipmentHeader[0].PK_OrderNo ?? "", // tbl_shipmentHeader.pk_orderNo as hwb/ tbl_confirmationCost.consolNo(if it is a consol)
      refNum2: confirmationCost[0].FK_OrderNo ?? "", // tbl_confirmationcost.fk_orderno as filenumber
    },
    shipmentDetails: {
      stops: [...pStopTypeData, ...dStopTypeData],
      dockHigh: "N", // req [Y / N]
      hazardous: shipmentDesc?.Hazmat ?? "N",
      liftGate: getLiftGate(shipmentApar?.ChargeCode ?? ""),
      unNum: getUnNum(shipmentDesc), // accepts only 4 degit number as string
      // notes: getNotes(shipmentInstructions, "S"),
    },
  };
  console.log("iviaPayload", JSON.stringify(iviaPayload));
  return iviaPayload;
};

module.exports = { loadP2PNonConsol };
