const GLOBAL_CONFIG = {
  // TODO 框架配置

  // base-url
  // VUE_APP_BASE_API: "http://192.168.0.19:8081",
  BASE_API: "http://localhost:5000/tower_server",
  // TODO: 业务服务器
  // VUE_APP_BASE_API: "http://10.1.26.146:81",
  // VUE_APP_BASE_API_2: "http://10.1.26.146:5000/tower_server",
  // TODO: 开发服务器
  VUE_APP_BASE_API: "http://localhost:5000/tower_server",
  VUE_APP_BASE_API_2: "http://localhost:5000/tower_server",
  // upload-base-url上传文件、图片的baseURL
  UPLOAD_BASE_API: "http://192.168.0.19:8081",

  /**
   * @type {boolean} true | false
   * @description 是否显示右侧设置按钮
   */
  // showSettings: process.env.NODE_ENV === 'development',
  showSettings: false,

  /**
   * @type {boolean} true | false
   * @description 是否显示右侧告警悬浮按钮
   */
  showWarning: true,

  /**
   * @type {boolean} true | false
   * @description 是否启用多标签
   */
  tagsView: false,

  /**
   * @type {boolean} true | false
   * @description 是否固定头部（默认初始不固定，引导功能完成后自动设置为true，解决header为fixed时引导页覆盖的bug）
   */
  fixedHeader: true,

  /**
   * @type {boolean} true | false
   * @description 是否显示SideBar上的Logo-Title组件
   */
  sidebarLogo: true,

  /**
   * @type {string | array} 'production' | ['production', 'development']
   * @description 是否显示错误日志组件。
   * 默认仅在生产环境中使用
   */
  errorLog: "production",

  // TODO 产品配置

  // 产品信息
  title: "测风塔管理系统",
  productLogo: "http://192.168.0.18:8808/logo.jpg",
  productLoginLogo: "http://192.168.0.18:8808/logo.png",
  copyright: "Copyright © 2024      有限公司      版权所有",

  // 不经过token校验的路由，白名单
  whiteList: ["/login", "/auth-redirect"],
  // token名称
  tokenName: "atm-auth",
  // token在localStorage、sessionStorage、cookie存储的key的名称前缀
  cookieCodePrefix: "ATM-SYS",
  // token失效回退到登录页时是否记录本次的路由
  recordRoute: false,
  // 最长请求时间
  requestTimeout: 200,
  // api接口请求成功code(Number)，默认200
  successCode: 200,
  // 无权限code
  noPermissionCode: 401,
  // vertical布局时是否只保持一个子菜单的展开
  uniqueOpened: false,
  // npm run build时是否自动生成gzip压缩包
  buildGzip: true
  // 是否开启登录RSA加密(未启用)
  // loginRSA: false,
  // 是否由后端生成菜单路由表(未启用)
  // generatorMenu: false,
  // 版本号(未启用)
  // version: process.env
};
// 做兼容性处理node/browser
if (typeof module !== 'undefined' && typeof module.exports !== 'undefined') {
  module.exports = GLOBAL_CONFIG
  // window.GLOBAL_CONFIG = GLOBAL_CONFIG
}
else {
  window.GLOBAL_CONFIG = GLOBAL_CONFIG
  // interface Window {
  //   GLOBAL_CONFIG: any
  // }
}
(function (window) {
  window.GLOBAL_CONFIG = GLOBAL_CONFIG

// })(typeof window == "undefined" ? global : window);
      // or
})(this);
