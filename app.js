const TelegramBot = require("node-telegram-bot-api");
const axios = require("axios");
const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");
const { Parser } = require("json2csv");
const cloudinary = require("cloudinary").v2;
const pLimit = require("p-limit");

// ================= CONFIG =================
const TELEGRAM_TOKEN = "8742242991:AAHDft6ZY7H7lMOuzFB7-zpMsr_nKYK2SHo";
const API_KEY = "4827f87b-0e70-45ac-b822-92e7b4d6a291";

const CONCURRENCY = 5; // parallel requests
const RETRY_COUNT = 3;

cloudinary.config({
  cloud_name: "dvsndenmu",
  api_key: "892768954865488",
  api_secret: "7SVc0KOWK_68zTgrQg7aCnTOlGc",
});

const API_URL = "https://l.creditlinks.in:8000/api/v2/partner/create-lead";

const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: true });

console.log("🚀 Bot Started");

// ================= UI =================

bot.onText(/\/start/, (msg) => {
  bot.sendMessage(msg.chat.id, "👋 Welcome!\nUpload CSV to create leads.", {
    reply_markup: {
      keyboard: [[{ text: "📤 Upload CSV" }]],
      resize_keyboard: true,
    },
  });
});

// ================= CORE FUNCTION =================

async function createLeadWithRetry(data) {
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

        consumerConsentDate: new Date()
          .toISOString()
          .slice(0, 19)
          .replace("T", " "),
        consumerConsentIp: "0.0.0.0",

        employmentStatus: 1,
        employerName: "Company",
        officePincode: data.pincode,

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

      await new Promise((r) => setTimeout(r, 1000)); // delay
    }
  }
}

// ================= FILE HANDLER =================

bot.on("document", async (msg) => {
  const chatId = msg.chat.id;

  bot.sendMessage(chatId, "📥 File received. Processing started...");

  try {
    const fileId = msg.document.file_id;
    const fileLink = await bot.getFileLink(fileId);

    const inputPath = path.join(__dirname, `input_${Date.now()}.csv`);
    const outputPath = path.join(__dirname, `output_${Date.now()}.csv`);

    // Download file
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
          let output = [];

          let success = 0;
          let failed = 0;
          let duplicate = 0;

          const limit = pLimit(CONCURRENCY);

          const tasks = rows.map((row) =>
            limit(async () => {
              const res = await createLeadWithRetry(row);

              if (res.includes("successfully")) success++;
              else if (res.includes("already")) duplicate++;
              else failed++;

              output.push({
                mobileNumber: row.mobileNumber,
                response: res,
              });

              console.log(row.mobileNumber, res);
            })
          );

          await Promise.all(tasks);

          // CSV OUTPUT
          const parser = new Parser();
          const csvData = parser.parse(output);

          fs.writeFileSync(outputPath, csvData);

          // Upload to Cloudinary
          const upload = await cloudinary.uploader.upload(outputPath, {
            resource_type: "raw",
          });

          // SEND RESULT
          bot.sendMessage(
            chatId,
            `✅ Processing Done!\n\n📊 Stats:\nTotal: ${rows.length}\nSuccess: ${success}\nDuplicate: ${duplicate}\nFailed: ${failed}\n\n📥 Download:\n${upload.secure_url}`
          );
        });
    });
  } catch (err) {
    console.error(err);
    bot.sendMessage(chatId, "❌ Error processing file");
  }
});