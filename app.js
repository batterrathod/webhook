const TelegramBot = require("node-telegram-bot-api");
const axios = require("axios");
const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");
const { Parser } = require("json2csv");
const cloudinary = require("cloudinary").v2;

// ================= YOUR KEYS =================
const TELEGRAM_TOKEN = "8742242991:AAHDft6ZY7H7lMOuzFB7-zpMsr_nKYK2SHo";
const API_KEY = "4827f87b-0e70-45ac-b822-92e7b4d6a291";

cloudinary.config({
  cloud_name: "dvsndenmu",
  api_key: "892768954865488",
  api_secret: "7SVc0KOWK_68zTgrQg7aCnTOlGc",
});

const API_URL = "https://l.creditlinks.in:8000/api/v2/partner/create-lead";

const RETRY_COUNT = 3;

const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: true });

// ================= STATS =================
let stats = {
  total: 0,
  processed: 0,
  success: 0,
  failed: 0,
  duplicate: 0,
};

console.log("🚀 Bot Started");

// ================= DATE FORMAT =================
function getFormattedDate() {
  const d = new Date();
  return d.getFullYear() + "-" +
    String(d.getMonth() + 1).padStart(2, "0") + "-" +
    String(d.getDate()).padStart(2, "0") + " " +
    String(d.getHours()).padStart(2, "0") + ":" +
    String(d.getMinutes()).padStart(2, "0") + ":" +
    String(d.getSeconds()).padStart(2, "0");
}

// ================= CSV → PAYLOAD =================
function convertRowToPayload(row) {
  return {
    mobileNumber: String(row.mobileNumber),
    firstName: String(row.firstName),
    lastName: String(row.lastName),
    pan: String(row.pan),
    dob: String(row.dob),
    email: String(row.email),
    pincode: String(row.pincode),
    monthlyIncome: Number(row.monthlyIncome),

    consumerConsentDate: getFormattedDate(),

    // 🔥 REAL IP
    consumerConsentIp: "103.21.58.192",

    employmentStatus: 1,
    employerName: row.employerName || "Private Ltd",
    officePincode: row.officePincode || row.pincode,

    waitForAllOffers: 1,
  };
}

// ================= VALIDATION =================
function validateRow(row) {
  if (!row.mobileNumber || row.mobileNumber.length !== 10) return "Invalid Mobile";
  if (!row.firstName) return "Missing First Name";
  if (!row.lastName) return "Missing Last Name";
  if (!row.pan) return "Missing PAN";
  if (!row.dob) return "Missing DOB";
  if (!row.email) return "Missing Email";
  if (!row.pincode) return "Missing Pincode";
  if (!row.monthlyIncome) return "Missing Income";
  return null;
}

// ================= CREATE LEAD =================
async function createLead(row) {
  let attempt = 0;

  while (attempt < RETRY_COUNT) {
    let payload;

    try {
      payload = convertRowToPayload(row);

      console.log("📤 Payload:", payload);

      const res = await axios.post(API_URL, payload, {
        headers: {
          apikey: API_KEY,
          "Content-Type": "application/json",
        },
      });

      return {
        message: res.data.message || "Success",
        leadId: res.data.leadId || "",
      };

    } catch (err) {
      attempt++;

      console.log("❌ ERROR STATUS:", err.response?.status);
      console.log("❌ ERROR DATA:", err.response?.data);

      let errorMsg = "Server Error";

      if (err.response) {
        if (err.response.status === 422) errorMsg = "Not eligible";
        else if (err.response.status === 400) errorMsg = err.response.data?.message || "Bad request";
      }

      if (attempt >= RETRY_COUNT) {
        return { message: errorMsg, leadId: "" };
      }

      await new Promise((r) => setTimeout(r, 1500));
    }
  }
}

// ================= PROCESS =================
async function processRows(rows, chatId) {
  let output = [];

  for (let i = 0; i < rows.length; i++) {
    const row = rows[i];

    const validationError = validateRow(row);

    if (validationError) {
      stats.failed++;
      stats.processed++;

      output.push({
        mobileNumber: row.mobileNumber,
        response: validationError,
        leadId: "",
      });

      continue;
    }

    const result = await createLead(row);

    const msg = result.message.toLowerCase();

    if (msg.includes("success")) stats.success++;
    else if (msg.includes("already")) stats.duplicate++;
    else stats.failed++;

    stats.processed++;

    output.push({
      mobileNumber: row.mobileNumber,
      response: result.message,
      leadId: result.leadId,
    });

    bot.sendMessage(chatId, `⏳ ${stats.processed}/${stats.total}`);
  }

  return output;
}

// ================= UI =================
bot.onText(/\/start/, (msg) => {
  bot.sendMessage(msg.chat.id, "👋 Upload CSV", {
    reply_markup: {
      keyboard: [
        [{ text: "📤 Upload CSV" }],
        [{ text: "📊 Stats" }]
      ],
      resize_keyboard: true,
    },
  });
});

bot.on("message", (msg) => {
  if (msg.text === "📊 Stats") {
    bot.sendMessage(
      msg.chat.id,
      `📊 Stats:\n\nTotal: ${stats.total}\nProcessed: ${stats.processed}\n✅ Success: ${stats.success}\n♻ Duplicate: ${stats.duplicate}\n❌ Failed: ${stats.failed}`
    );
  }
});

// ================= MAIN =================
bot.on("document", async (msg) => {
  const chatId = msg.chat.id;

  bot.sendMessage(chatId, "📥 Processing started...");

  stats = { total: 0, processed: 0, success: 0, failed: 0, duplicate: 0 };

  try {
    const fileLink = await bot.getFileLink(msg.document.file_id);

    const inputPath = path.join(__dirname, "input.csv");
    const outputPath = path.join(__dirname, "output.csv");

    const writer = fs.createWriteStream(inputPath);
    const response = await axios({
      url: fileLink,
      method: "GET",
      responseType: "stream",
    });

    response.data.pipe(writer);

    writer.on("finish", async () => {
      let rows = [];

      fs.createReadStream(inputPath)
        .pipe(csv())
        .on("data", (data) => rows.push(data))
        .on("end", async () => {
          stats.total = rows.length;

          const output = await processRows(rows, chatId);

          const parser = new Parser();
          const csvData = parser.parse(output);

          fs.writeFileSync(outputPath, csvData);

          const upload = await cloudinary.uploader.upload(outputPath, {
            resource_type: "raw",
          });

          bot.sendMessage(
            chatId,
            `✅ Done!\n\n📊 Total: ${stats.total}\nProcessed: ${stats.processed}\n✅ Success: ${stats.success}\n♻ Duplicate: ${stats.duplicate}\n❌ Failed: ${stats.failed}\n\n📥 ${upload.secure_url}`
          );
        });
    });

  } catch (err) {
    console.error(err);
    bot.sendMessage(chatId, "❌ Error");
  }
});