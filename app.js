const TelegramBot = require("node-telegram-bot-api");
const axios = require("axios");
const csv = require("csv-parser");
const { Parser } = require("json2csv");
const cloudinary = require("cloudinary").v2;
const streamifier = require("streamifier");
const { Readable } = require("stream");

// 🔐 PUT YOUR KEYS HERE
const TELEGRAM_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN";
const API_KEY = "YOUR_CREDITLINKS_API_KEY";

const CLOUD_NAME = "YOUR_CLOUD_NAME";
const CLOUD_API_KEY = "YOUR_CLOUD_API_KEY";
const CLOUD_API_SECRET = "YOUR_CLOUD_API_SECRET";

// CLOUDINARY CONFIG
cloudinary.config({
  cloud_name: CLOUD_NAME,
  api_key: CLOUD_API_KEY,
  api_secret: CLOUD_API_SECRET
});

// TELEGRAM BOT
const bot = new TelegramBot(TELEGRAM_TOKEN, { polling: true });

// API URL
const API_URL = "https://l.creditlinks.in:8000/api/v2/partner/create-lead";

// CREATE LEAD FUNCTION
async function createLead(row) {
  try {
    const payload = {
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
    };

    const res = await axios.post(API_URL, payload, {
      headers: {
        apikey: API_KEY,
        "Content-Type": "application/json"
      }
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

// START
bot.onText(/\/start/, (msg) => {
  bot.sendMessage(msg.chat.id, "📤 Upload your CSV file");
});

// HANDLE CSV FILE
bot.on("document", async (msg) => {
  const chatId = msg.chat.id;

  try {
    bot.sendMessage(chatId, "📥 Processing started...");

    // GET FILE
    const file = await bot.getFile(msg.document.file_id);
    const fileUrl = `https://api.telegram.org/file/bot${TELEGRAM_TOKEN}/${file.file_path}`;

    // DOWNLOAD FILE AS BUFFER
    const response = await axios.get(fileUrl, { responseType: "arraybuffer" });

    // PARSE CSV
    const rows = [];
    const stream = Readable.from(response.data);

    await new Promise((resolve, reject) => {
      stream
        .pipe(csv())
        .on("data", (data) => rows.push(data))
        .on("end", resolve)
        .on("error", reject);
    });

    const results = [];

    for (let i = 0; i < rows.length; i++) {
      const result = await createLead(rows[i]);
      results.push(result);

      // small delay
      await new Promise(r => setTimeout(r, 400));

      if (i % 5 === 0) {
        bot.sendMessage(chatId, `⏳ ${i + 1}/${rows.length}`);
      }
    }

    // CREATE OUTPUT CSV
    const parser = new Parser();
    const csvData = parser.parse(results);
    const buffer = Buffer.from(csvData);

    // UPLOAD TO CLOUDINARY
    const uploadResult = await new Promise((resolve, reject) => {
      const stream = cloudinary.uploader.upload_stream(
        {
          resource_type: "raw",
          folder: "telegram-leads",
          public_id: `output_${Date.now()}`
        },
        (error, result) => {
          if (error) reject(error);
          else resolve(result);
        }
      );

      streamifier.createReadStream(buffer).pipe(stream);
    });

    // SEND LINK
    await bot.sendMessage(
      chatId,
      `✅ Done!\n📥 Download: ${uploadResult.secure_url}`
    );

    // SEND FILE ALSO
    await bot.sendDocument(chatId, buffer, {}, {
      filename: "output.csv",
      contentType: "text/csv"
    });

  } catch (err) {
    console.error(err);
    bot.sendMessage(chatId, "❌ Error processing file");
  }
});

// ERROR HANDLING
process.on("unhandledRejection", console.error);