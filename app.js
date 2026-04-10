const TelegramBot = require("node-telegram-bot-api");
const axios = require("axios");
const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");
const { Parser } = require("json2csv");
const cloudinary = require("cloudinary").v2;

// ================= HARDCODED KEYS =================
const TELEGRAM_TOKEN = "8742242991:AAHDft6ZY7H7lMOuzFB7-zpMsr_nKYK2SHo";
const API_KEY = "4827f87b-0e70-45ac-b822-92e7b4d6a291";

const CONCURRENCY = 5;
const RETRY_COUNT = 3;

cloudinary.config({
  cloud_name: "dvsndenmu",
  api_key: "892768954865488",
  api_secret: "7SVc0KOWK_68zTgrQg7aCnTOlGc",
});

const API_URL = "https://l.creditlinks.in:8000/api/v2/partner/create-lead";

const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: true });

// ================= GLOBAL STATS =================
let stats = {
  total: 0,
  processed: 0,
  success: 0,
  failed: 0,
  duplicate: 0,
};

console.log("🚀 Bot Started");

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

// ================= RETRY =================
async function createLead(data) {
  let attempt = 0;

  while (attempt < RETRY_COUNT) {
    try {
      const payload = {
        mobileNumber: data.mobileNumber,
        firstName: data.firstName,
        lastName: data.lastName,
        pan: data.pan,
        dob: data.dob,
        email: data.email,
        pincode: data.pincode,
        monthlyIncome: parseInt(data.monthlyIncome),

        consumerConsentDate:
          data.consumerConsentDate ||
          new Date().toISOString().slice(0, 19).replace("T", " "),
        consumerConsentIp: "0.0.0.0",

        employmentStatus: 1,
        employerName: data.employerName || "Company",
        officePincode: data.officePincode || data.pincode,

        waitForAllOffers: 1,
      };

      const res = await axios.post(API_URL, payload, {
        headers: {
          apikey: API_KEY,
          "Content-Type": "application/json",
        },
      });

      return res.data.message || "Success";
    } catch (err) {
      attempt++;
      if (attempt >= RETRY_COUNT) {
        return err.response?.data?.message || "Failed";
      }
      await new Promise((r) => setTimeout(r, 1000));
    }
  }
}

// ================= BATCH PROCESS =================
async function processBatches(rows, chatId) {
  let output = [];

  for (let i = 0; i < rows.length; i += CONCURRENCY) {
    const batch = rows.slice(i, i + CONCURRENCY);

    const results = await Promise.all(
      batch.map(async (row) => {
        const res = await createLead(row);

        stats.processed++;

        if (res.toLowerCase().includes("success")) stats.success++;
        else if (res.toLowerCase().includes("already")) stats.duplicate++;
        else stats.failed++;

        return {
          mobileNumber: row.mobileNumber,
          response: res,
        };
      })
    );

    output.push(...results);

    // progress update
    bot.sendMessage(
      chatId,
      `⏳ Progress: ${stats.processed}/${stats.total}`
    );
  }

  return output;
}

// ================= MAIN =================
bot.on("document", async (msg) => {
  const chatId = msg.chat.id;

  bot.sendMessage(chatId, "📥 Processing started...");

  // reset stats
  stats = { total: 0, processed: 0, success: 0, failed: 0, duplicate: 0 };

  try {
    const fileId = msg.document.file_id;
    const fileLink = await bot.getFileLink(fileId);

    const inputPath = path.join(__dirname, `input_${Date.now()}.csv`);
    const outputPath = path.join(__dirname, `output_${Date.now()}.csv`);

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

          const output = await processBatches(rows, chatId);

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
    bot.sendMessage(chatId, "❌ Error processing file");
  }
});