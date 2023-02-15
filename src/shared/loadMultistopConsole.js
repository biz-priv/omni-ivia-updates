const { getLiftGate, getUnNum } = require("./dataHelper");
const moment = require("moment");

/**
 * multistop console //non consol p2p // send console no
 */
const loadMultistopConsole = (dataSet, shipmentAparData) => {
  console.log("loadMultistopConsole");
  const confirmationCost = dataSet.confirmationCost;
  const consolStopHeaders = dataSet.consolStopHeaders;
  const consolStopItems = dataSet.consolStopItems;
  const shipmentApar = shipmentAparData;
  const shipmentHeader = dataSet.shipmentHeader;
  //   shipmentInstructions

  const filteredConfirmationCost = confirmationCost.filter((e) => {
    return (
      e.FK_OrderNo === shipmentApar.FK_OrderNo &&
      e.FK_SeqNo === shipmentApar.SeqNo &&
      shipmentApar.Consolidation === "N"
    );
  });

  const housebill_delimited = shipmentHeader
    .filter((e) => {
      const conHeaders = consolStopHeaders
        .filter(
          (e) =>
            e.FK_ConsolNo === shipmentApar.ConsolNo && e.ConsolStopNumber === 0
        )
        .map((e) => e.PK_ConsolStopId);
      const orderNoList = consolStopItems
        .filter((e) => conHeaders.includes(e.FK_ConsolStopId))
        .map((e) => e.FK_OrderNo);
      return orderNoList.includes(e.PK_OrderNo);
    })
    .map((e) => e.Housebill);

  const cargo = dataSet.shipmentDesc
    .filter((e) => {
      const conHeaders = consolStopHeaders
        .filter(
          (e) =>
            e.FK_ConsolNo === shipmentApar.ConsolNo && e.ConsolStopNumber === 0
        )
        .map((e) => e.PK_ConsolStopId);
      const orderNoList = consolStopItems
        .filter((e) => conHeaders.includes(e.FK_ConsolStopId))
        .map((e) => e.FK_OrderNo);
      return orderNoList.includes(e.FK_OrderNo);
    })
    .map((e) => {
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

  const shipmentDetailsStops = consolStopItems
    .filter(
      (e) =>
        shipmentApar.Consolidation === "N" &&
        e.FK_ConsolNo === shipmentApar.ConsolNo
    )
    .map((e) => {
      //for every consolStopItems we will have only one consoleStopHeader data
      const csh =
        consolStopHeaders.filter(
          (e_csh) => e_csh.PK_ConsolStopId === e.FK_ConsolStopId
        )?.[0] ?? {};

      let shipmentInstructions =
        dataSet.shipmentInstructions.length > 0
          ? dataSet.shipmentInstructions
          : [];

      //ConsolStopPickupOrDelivery (false = P, true = D)
      if (csh.ConsolStopPickupOrDelivery === "false") {
        // filter based on type
        shipmentInstructions = shipmentInstructions
          .filter((e) => {
            return e.Type === "P";
          })
          .map((e) => e.Note);
        shipmentInstructions =
          shipmentInstructions.length > 0 ? shipmentInstructions.join(" ") : "";
        return {
          stopType: "P",
          stopNum: 0,
          housebills: housebill_delimited,
          address: {
            address1: e.ConsolStopAddress1,
            city: e.ConsolStopCity,
            country: e.FK_ConsolStopCountry,
            state: e.FK_ConsolStopState,
            zip: e.ConsolStopZip,
          },
          companyName: csh?.ConsolStopName,
          cargo: cargo,
          scheduledDate: moment(
            e.ConsolStopDate.split(" ")[0] +
              " " +
              e.ConsolStopTimeBegin.split(" ")[1]
          ).diff("1970-01-01", "ms"),
          specialInstructions:
            (e.ConsolStopAddress2 === "" ? "" : e.ConsolStopAddress2 + " ") +
            shipmentInstructions,
        };
      } else {
        shipmentInstructions = shipmentInstructions
          .filter((e) => {
            return e.Type === "D";
          })
          .map((e) => e.Note);
        shipmentInstructions =
          shipmentInstructions.length > 0 ? shipmentInstructions.join(" ") : "";
        return {
          stopType: "D",
          stopNum: 1,
          housebills: housebill_delimited,
          address: {
            address1: e.ConsolStopAddress1,
            city: e.ConsolStopCity,
            country: e.FK_ConsolStopCountry,
            state: e.FK_ConsolStopState,
            zip: e.ConsolStopZip,
          },
          companyName: csh?.ConsolStopName,
          scheduledDate: moment(
            e.ConsolStopDate.split(" ")[0] +
              " " +
              e.ConsolStopTimeBegin.split(" ")[1]
          ).diff("1970-01-01", "ms"),
          specialInstructions:
            (e.ConsolStopAddress2 === "" ? "" : e.ConsolStopAddress2 + " ") +
            shipmentInstructions,
        };
      }
    });

  const iviaPayload = {
    carrierId: process.env.IVIA_CARRIER_ID, // process.env.IVIA_CARRIER_ID = 1000025
    refNums: {
      refNum1: filteredConfirmationCost[0].ConsolNo ?? "", // tbl_shipmentHeader.pk_orderNo as hwb/ tbl_confirmationCost.consolNo(if it is a consol)
      refNum2: "", // as query filenumber value is always "" hardcode
    },
    shipmentDetails: {
      stops: shipmentDetailsStops,
      dockHigh: "N", // req [Y / N]
      hazardous: shipmentDesc?.Hazmat ?? "N",
      liftGate: getLiftGate(shipmentApar?.ChargeCode ?? ""),
      unNum: getUnNum(shipmentDesc.shipmentDesc), // accepts only 4 degit number as string
    },
  };
  console.log("iviaPayload", JSON.stringify(iviaPayload));
  return iviaPayload;
};
module.exports = { loadMultistopConsole };
