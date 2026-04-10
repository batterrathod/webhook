const TelegramBot = require("node-telegram-bot-api");
const axios = require("axios");
const csv = require("csv-parser");
const { Parser } = require("json2csv");
const cloudinary = require("cloudinary").v2;
const streamifier = require("streamifier");
const { Readable } = require("stream");

// 🔐PUT YOUR KEYS HERE
const TELEGRAM_TOKEN = "8742242991:AAEk8TO6sOgA1uinP9ZXdYp-g5yXDkDLsBI";
const API_KEY = "4827f87b-0e70-45ac-b822-92e7b4d6a291";

const CLOUD_NAME = "dvsndenmu";
const CLOUD_API_KEY = "892768954865488";
const CLOUD_API_SECRET = "7SVc0KOWK_68zTgrQg7aCnTOlGc";
P
// ⚡ SPEED CONFIG
const CONCURRENCY = 10;

// CLOUDINARY
cloudinary.config({
  cloud_name: CLOUD_NAME,
  api_key: CLOUD_API_KEY,
  api_secret: CLOUD_API_SECRET
});

// TELEGRAM BOT (FIX 409)
const bot = new TelegramBot(TELEGRAM_TOKEN);
bot.deleteWebHook().then(() => {
  bot.startPolling();
});

// ⚡ AXIOS INSTANCE (FASTER)
const api = axios.create({
  baseURL: "https://l.creditlinks.in:8000/api/v2/partner/create-lead",
  headers: {
    apikey: API_KEY,
    "Content-Type": "application/json"
  },
  timeout: 10000
});

// CREATE LEAD
async function createLead(row) {
  try {
    const res = await api.post("", {
      mobileNumber: row.mobileNumber,
      firstName: row.firstName,
      lastName: row.lastName,
      pan: row.pan,
      dob: row.dob,
      email: row.email,
      pincode: row.pincode,
      monthlyIncome: parseInt(row.monthlyIncome),
      employmentStatus: parseInt(row.employmentStatus),
      employerName: row.employerName,
      officePincode: row.officePincode,
      consumerConsentDate: new Date()
        .toISOString()
        .slice(0, 19)
        .replace("T", " "),
      consumerConsentIp: "127.0.0.1",
      waitForAllOffers: 1
    });

    return {
      ...row,
      success: res.data.success,
      message: res.data.message,
      leadId: res.data.leadId || ""
    };

  } catch (err) {
    return {
      ...row,
      success: false,
      message: err.response?.data?.message || err.message,
      leadId: ""
    };
  }
}

// ⚡ FAST BATCH PROCESS
async function processBatch(rows) {
  let results = [];

  for (let i = 0; i < rows.length; i += CONCURRENCY) {
    const batch = rows.slice(i, i + CONCURRENCY);

    const batchResults = await Promise.all(batch.map(createLead));
    results.push(...batchResults);

    console.log(`Processed ${Math.min(i + CONCURRENCY, rows.length)}/${rows.length}`);
  }

  return results;
}

// START
bot.onText(/\/start/, (msg) => {
  bot.sendMessage(msg.chat.id, "📤 Upload your CSV file");
});

// HANDLE FILE
bot.on("document", async (msg) => {
  const chatId = msg.chat.id;

  try {
    bot.sendMessage(chatId, "⚡ Processing started...");

    // GET FILE
    const file = await bot.getFile(msg.document.file_id);
    const fileUrl = `https://api.telegram.org/file/bot${TELEGRAM_TOKEN}/${file.file_path}`;

    // DOWNLOAD
    const response = await axios.get(fileUrl, {
      responseType: "arraybuffer"
    });

    // PARSE CSV
    const rows = [];
    const stream = Readable.from(response.data);

    await new Promise((resolve, reject) => {
      stream.pipe(csv())
        .on("data", (data) => rows.push(data))
        .on("end", resolve)
        .on("error", reject);
    });

    await bot.sendMessage(chatId, `📊 Total Leads: ${rows.length}`);

    // ⚡ FAST PROCESS
    const results = await processBatch(rows);

    // CREATE CSV
    const parser = new Parser();
    const buffer = Buffer.from(parser.parse(results));

    // UPLOAD CLOUDINARY
    const uploadResult = await new Promise((resolve, reject) => {
      const uploadStream = cloudinary.uploader.upload_stream(
        {
          resource_type: "raw",
          folder: "telegram-leads",
          public_id: `output_${Date.now()}`
        },
        (err, res) => err ? reject(err) : resolve(res)
      );

      streamifier.createReadStream(buffer).pipe(uploadStream);
    });

    // SEND RESULT
    await bot.sendMessage(chatId, `✅ Done!\n📥 ${uploadResult.secure_url}`);

    await bot.sendDocument(chatId, buffer, {}, {
      filename: "output.csv"
    });

  } catch (err) {
    console.error(err);
    bot.sendMessage(chatId, "❌ Error processing file");
  }
});

// ERROR HANDLER
process.on("unhandledRejection", console.error);