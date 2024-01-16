const sql = require("mssql");
const { format } = require("date-fns");

// // SỬA CẤU HÌNH THEO DB BÊN BCCP CẤP CMT LÀ ĐANG LOCALHOST
// const configA = {
//   user: "quoctb",
//   password: "quoctb@123",
//   server: "localhost",
//   database: "FULL_ITEMS",
//   options: {
//     encrypt: false,
//   },
// };

// BD BÊN BCCP CẤP
const configA = {
  user: "bccpcom_bi_cms",
  password: "Bccpc0m@2023",
  server: "172.16.252.35",
  database: "BCCPCom",
  options: {
    encrypt: false,
  },
};


// DB PRODUCT
const configB = {
  user: "monitorvnpost",
  password: "monitor@12",
  server: "172.16.128.3",
  database: "CAS2BCCP",
  options: {
    encrypt: false,
  },
};

// //DB Test Local
// const configB = {
//   user: "quoctb",
//   password: "quoctb@123",
//   server: "localhost",
//   database: "CAS2BCCP",
//   options: {
//     encrypt: false,
//   },
// };

// Tạo pool kết nối cho Database A
const poolA = new sql.ConnectionPool(configA);

// Tạo pool kết nối cho Database B
const poolB = new sql.ConnectionPool(configB);

// Hàm kết nối đến Database A
async function connectToDatabaseA() {
  try {
    await poolA.connect();
    console.log("Connected to ParcelID List");
  } catch (error) {
    console.error("Error connecting to Database A:", error);
    throw error;
  }
}

// Hàm đóng kết nối với Database A
async function closeConnectionToDatabaseA() {
  await poolA.close();
}

// Hàm kết nối đến Database B
async function connectToDatabaseB() {
  try {
    await poolB.connect();
    console.log("Connected to DI System");
  } catch (error) {
    console.error("Error connecting to Database B:", error);
    throw error;
  }
}

// Hàm đóng kết nối với Database B
async function closeConnectionToDatabaseB() {
  await poolB.close();
}

// Hàm lấy số lượng Item dựa trên ParcelID từ Database B
async function getItemsByParcelID(parcelID) {
  try {
    const request = poolB.request();
    const result = await request.query(`
      SELECT COUNT(*) AS parcel_total
      FROM ${configB.database}.dbo.Item
      WHERE ItemCode = '${parcelID}'
    `);

    return result.recordset[0].parcel_total;
  } catch (error) {
    console.error("Error:", error);
    throw error;
  }
}

// Hàm chính để thực hiện việc chuyển dữ liệu
async function transferData() {
  try {
    await connectToDatabaseA();

    const result = await poolA
      .request()
      .query("SELECT parcel_id FROM ITEM_LIST WHERE parcel_status = '0'"); //LẤY HẾT TẤT CẢ TRẠNG THÁI 0 1 2 / TRƯỜNG HỢP KHÔNG MUỐN BẮN LẶP LẠI THÌ THÊM WHERE = '0'
    const parcelIds = result.recordset.map((row) => row.parcel_id);
    console.log(parcelIds);

    await connectToDatabaseB();

    for (let i = 0; i < parcelIds.length; i++) {
      const rowCount = await getItemsByParcelID(parcelIds[i]);
      console.log("rowCount of ", parcelIds[i], ":", rowCount);
      if (rowCount > 0) {
        console.log(parcelIds[i], " Có trên trục");
        // Thực hiện truy vấn INSERT vào Item2BCCPCOMQueueKT1 nếu tồn tại
        const insertQuery = `
          INSERT INTO [${configB.database}].[dbo].[Item2BCCPCOMQueueKT1]
          SELECT id, itemcode, AcceptancePOSCode, mailstxnid, 'I', GETDATE()
          FROM [${configB.database}].[dbo].[Item]
          WHERE ItemCode = '${parcelIds[i]}'
          `;
        await poolB.request().query(insertQuery);

        const updateQuery = `
          UPDATE ${configA.database}.dbo.ITEM_LIST
          SET parcel_status = 1, update_time = GETDATE()
          WHERE parcel_id = '${parcelIds[i]}'
          `;
        await poolA.request().query(updateQuery);
      } 
      else 
      {
        console.log(parcelIds[i], "Không có trên trục");
        const updateQuery = `
          UPDATE ${configA.database}.dbo.ITEM_LIST
          SET parcel_status = 2, update_time = GETDATE()
          WHERE parcel_id = '${parcelIds[i]}'
          `;
        await poolA.request().query(updateQuery);
      }
    }

    const currentDate = new Date();
    const formattedDate = format(currentDate, "yyyy-MM-dd HH:mm:ss");

    console.log("Job run is Successfully at ", formattedDate);
    console.log("----------------------------------------------------");
  } catch (err) {
    console.error("[Job Error]:", err);
  } finally {
    await closeConnectionToDatabaseA();
    await closeConnectionToDatabaseB();
  }
}

// Chạy hàm transferData sau mỗi 30 giây
setInterval(transferData, 5 * 60 * 1000);
