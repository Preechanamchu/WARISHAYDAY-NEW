// netlify/functions/get-customer-data.js
const db = require('./database');
const { readProductMachines } = require('./product-machines-db');

// นี่คือ API สาธารณะ ไม่จำเป็นต้องมีการยืนยันตัวตน (login)
exports.handler = async (event, context) => {
  // อนุญาตเฉพาะการร้องขอแบบ GET เท่านั้น
  if (event.httpMethod !== 'GET') {
    return { statusCode: 405, body: 'Method Not Allowed' };
  }

  try {
    // These queries do not depend on one another. Running them together removes
    // two unnecessary database round trips from the customer startup path.
    const [categoriesResult, productsResult, settingsResult] = await Promise.all([
      db.query('SELECT * FROM categories ORDER BY sort_order ASC'),
      db.query('SELECT * FROM products ORDER BY category_id, level ASC'),
      db.query('SELECT settings_json FROM shop_settings WHERE id = 1')
    ]);
    const allShopSettings = settingsResult.rows[0]?.settings_json || {};
    const productMachines = await readProductMachines(db, allShopSettings.productMachines || []);

    // 4. *** สำคัญมาก ***
    // สร้าง object 'shopSettings' ที่ "ปลอดภัย" เพื่อส่งให้ลูกค้า
    // เราต้องคัดกรองข้อมูลที่ละเอียดอ่อน (เช่น โค้ดส่วนลด, ข้อมูลภายใน) ออกไป
    const safeShopSettings = {
      // ข้อมูลแบรนด์และธีม
      shopName: allShopSettings.shopName,
      slogan: allShopSettings.slogan,
      shopNameColor: allShopSettings.shopNameColor,
      sloganColor: allShopSettings.sloganColor,
      themeName: allShopSettings.themeName,
      // A disabled Base64 logo can be hundreds of KB and is not rendered by customers.
      // Keep it available in Admin API, but omit it from the public payload until enabled.
      logo: allShopSettings.useLogo ? allShopSettings.logo : '',
      useLogo: allShopSettings.useLogo,
      darkMode: allShopSettings.darkMode,

      // ฟอนต์และการปรับขนาด
      fontFamily: allShopSettings.fontFamily,
      globalFontFamily: allShopSettings.globalFontFamily,
      globalFontSize: allShopSettings.globalFontSize,
      mainMenuFontSize: allShopSettings.mainMenuFontSize,
      subMenuFontSize: allShopSettings.subMenuFontSize,
      shopNameFontSize: allShopSettings.shopNameFontSize,
      sloganFontSize: allShopSettings.sloganFontSize,
      sloganFontFamily: allShopSettings.sloganFontFamily,

      // เอฟเฟกต์
      shopNameEffect: allShopSettings.shopNameEffect,
      sloganEffect: allShopSettings.sloganEffect,
      logoEffect: allShopSettings.logoEffect,
      effects: allShopSettings.effects, // เอฟเฟกต์เทศกาล (ปลอดภัย)

      // พื้นหลังและลิขสิทธิ์
      backgroundImage: allShopSettings.backgroundImage,
      backgroundOpacity: allShopSettings.backgroundOpacity,
      backgroundBlur: allShopSettings.backgroundBlur,
      copyrightText: allShopSettings.copyrightText,
      copyrightOpacity: allShopSettings.copyrightOpacity,

      // --- ส่วนที่อัปเดตตามคำขอ ---
      // สถานะร้านค้าและข้อความ (สำหรับลูกค้า)
      shopEnabled: allShopSettings.shopEnabled,
      announcementEnabled: allShopSettings.announcementEnabled,
      shopClosedMessageText: allShopSettings.shopClosedMessageText,
      announcementMessageText: allShopSettings.announcementMessageText,
      messageSettings: allShopSettings.messageSettings, // (รวมการตั้งค่าข้อความวิ่ง)

      // --- เพิ่มการตั้งค่า เปิด/ปิด การสมัครสมาชิก ---
      registrationEnabled: allShopSettings.registrationEnabled ?? true,

      // UI Layout (สำหรับลูกค้า)
      salesMode: allShopSettings.salesMode,
      orderBarSettings: allShopSettings.orderBarSettings, // (การตั้งค่าแถบสั่งซื้อ)
      gridLayoutSettings: allShopSettings.gridLayoutSettings, // (การตั้งค่ากริดสินค้า)
      priceTagConfig: allShopSettings.priceTagConfig, // (การตั้งค่าป้ายราคา 🏷️)
      priceTagUpgradeConfig: allShopSettings.priceTagUpgradeConfig || { closingMessage: '', fontSize: 50 },
      priceTagCoinConfig: allShopSettings.priceTagCoinConfig || { closingMessage: '', fontSize: 50 },
      priceTagDiamondConfig: allShopSettings.priceTagDiamondConfig || { closingMessage: '', fontSize: 50 },
      priceTagVoucherConfig: allShopSettings.priceTagVoucherConfig || { closingMessage: '', fontSize: 50 },
      priceTagProductMachinesConfig: allShopSettings.priceTagProductMachinesConfig || { closingMessage: '', fontSize: 50 },
      // --- จบส่วนอัปเดต ---

      // UI อื่นๆ
      loadingScreen: allShopSettings.loadingScreen,
      successAnimation: allShopSettings.successAnimation,
      language: allShopSettings.language,

      // --- แพ็กเกจสินค้าเสมือน (เหรียญ / เพชร / Farm Pass / บัตรกำนัล) ---
      // ข้อมูลเหล่านี้คือ catalog สินค้าที่ลูกค้าต้องเห็น ไม่ใช่ข้อมูลละเอียดอ่อน
      coinPackages: allShopSettings.coinPackages || [],
      diamondPackages: allShopSettings.diamondPackages || [],
      farmPassPackages: allShopSettings.farmPassPackages || [],
      voucherPackages: allShopSettings.voucherPackages || [],
      upgradeSettings: allShopSettings.upgradeSettings || {},
      productMachines: productMachines,
      showcaseSettings: allShopSettings.showcaseSettings || {
        selectedProductIds: [],
        categories: {},
        maxItems: 10,
        effect: { enabled: false, type: 'confetti', intensity: 30 }
      },
      copyrightFontSize: allShopSettings.copyrightFontSize,

      // --- ลำดับการแสดงผลหน้าแคตตาล็อก (สำหรับลูกค้า) ---
      stockSubMenuOrder: allShopSettings.stockSubMenuOrder,
      // --- การมองเห็นหน้า catalog (เปิด/ปิด) ---
      catalogVisibility: allShopSettings.catalogVisibility,

      // --- พื้นหลังแต่ละหน้าสินค้า (Section Backgrounds) ---
      sectionBackgrounds: allShopSettings.sectionBackgrounds,

      // --- วิดีโอแนะนำวิธีดูแท็ก/เมล/เหรียญ ---
      tagTutorialVideoUrl: allShopSettings.tagTutorialVideoUrl,
      tagTutorialVideoFile: allShopSettings.tagTutorialVideoFile,
      mailTutorialVideoUrl: allShopSettings.mailTutorialVideoUrl,
      mailTutorialVideoFile: allShopSettings.mailTutorialVideoFile,
      coinTutorialVideoUrl: allShopSettings.coinTutorialVideoUrl,
      coinTutorialVideoFile: allShopSettings.coinTutorialVideoFile

      // *** ข้อมูลที่ถูกคัดกรองออก (ไม่ส่งให้ลูกค้า) ***
      // - promotions (มีโค้ดส่วนลดทั้งหมด)
      // - orderNumberCounters (ตรรกะภายใน)
      // - managerName, shareholderName (ข้อมูลส่วนตัว)
      // - และข้อมูลอื่นๆ เฉพาะสำหรับแอดมิน
    };

    // 5. รวบรวมข้อมูลทั้งหมดเพื่อส่งกลับไป
    const customerData = {
      categories: categoriesResult.rows,
      products: productsResult.rows,
      shopSettings: safeShopSettings // ส่งเฉพาะข้อมูลที่ปลอดภัย
    };

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache, no-store, must-revalidate',
        'Pragma': 'no-cache',
        'Expires': '0'
      },
      body: JSON.stringify(customerData),
    };

  } catch (error) {
    console.error('Error in get-customer-data function:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({ error: 'Failed to fetch customer data.' }),
    };
  }
};
