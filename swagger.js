const swaggerJsdoc = require('swagger-jsdoc');
const swaggerUi = require('swagger-ui-express');

const options = {
  definition: {
    openapi: '3.0.0',
    info: {
      title: 'Vision Terminal Stock Portfolio API',
      version: '1.0.0',
      description: 'API documentation for the Vision Terminal Stock Portfolio backend',
      contact: {
        name: 'Developer Support',
        email: 'support@visionterminal.local',
      },
    },
    servers: [
      {
        url: 'http://localhost:5001',
        description: 'Development Server',
      },
    ],
    components: {
      securitySchemes: {
        BearerAuth: {
          type: 'http',
          scheme: 'bearer',
          bearerFormat: 'JWT',
          description: 'Enter your JWT token in the format Bearer <token>',
        },
      },
      schemas: {
        User: {
          type: 'object',
          properties: {
            _id: { type: 'string', example: '60c72b2f9b1d8b2bad8d3e4a' },
            email: { type: 'string', example: 'user@example.com' },
            fullName: { type: 'string', example: 'John Doe' },
            emailVerified: { type: 'boolean', example: false },
            role: { type: 'string', example: 'user' },
            createdAt: { type: 'string', format: 'date-time' },
          },
        },
        Watchlist: {
          type: 'object',
          properties: {
            _id: { type: 'string', example: '60c72b2f9b1d8b2bad8d3e4f' },
            name: { type: 'string', example: 'default' },
            isDefault: { type: 'boolean', example: true },
            userId: { type: 'string', example: '60c72b2f9b1d8b2bad8d3e4a' },
            createdAt: { type: 'string', format: 'date-time' },
            updatedAt: { type: 'string', format: 'date-time' },
          },
        },
        Stock: {
          type: 'object',
          required: ['symbol', 'name'],
          properties: {
            _id: { type: 'string', example: '60c72b2f9b1d8b2bad8d3e50' },
            symbol: { type: 'string', example: 'RELIANCE.NS' },
            name: { type: 'string', example: 'Reliance Industries Limited' },
            isFavourite: { type: 'boolean', example: false },
            watchlist: { type: 'string', example: 'default' },
            tags: { type: 'array', items: { type: 'string' }, example: ['watchlist1'] },
            userId: { type: 'string', example: '60c72b2f9b1d8b2bad8d3e4a' },
          },
        },
        CorporateAction: {
          type: 'object',
          properties: {
            id: { type: 'string', example: 'act-dyn-0' },
            symbol: { type: 'string', example: 'INFY.NS' },
            companyName: { type: 'string', example: 'INFY Limited' },
            type: { type: 'string', enum: ['dividend', 'split', 'bonus', 'buyback'], example: 'dividend' },
            ratioOrAmount: { type: 'string', example: '₹18.00 per share' },
            exDate: { type: 'string', format: 'date', example: '2026-06-05' },
            recordDate: { type: 'string', format: 'date', example: '2026-06-07' },
            status: { type: 'string', enum: ['Upcoming', 'Completed'], example: 'Upcoming' },
            description: { type: 'string', example: 'Final dividend recommended by the board of INFY.' },
          },
        },
        Notification: {
          type: 'object',
          properties: {
            _id: { type: 'string', example: '60c72b2f9b1d8b2bad8d3e55' },
            userId: { type: 'string', example: '60c72b2f9b1d8b2bad8d3e4a' },
            title: { type: 'string', example: 'Alert Triggered: INFY' },
            message: { type: 'string', example: 'INFY price crosses ₹1500' },
            isRead: { type: 'boolean', example: false },
            type: { type: 'string', example: 'alert' },
            createdAt: { type: 'string', format: 'date-time' },
          },
        },
        CustomTag: {
          type: 'object',
          properties: {
            tagId: { type: 'string', example: 'watchlist1' },
            label: { type: 'string', example: 'Watchlist 1' },
            color: { type: 'string', example: '#f97316' },
          },
        },
      },
    },
    paths: {
      '/api/auth/register': {
        post: {
          tags: ['Authentication'],
          summary: 'Register a new user account',
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  required: ['email', 'password', 'fullName'],
                  properties: {
                    email: { type: 'string', format: 'email', example: 'user@example.com' },
                    password: { type: 'string', format: 'password', example: 'Password123!' },
                    fullName: { type: 'string', example: 'John Doe' },
                  },
                },
              },
            },
          },
          responses: {
            201: {
              description: 'User registered successfully',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      message: { type: 'string' },
                      token: { type: 'string' },
                      user: { $ref: '#/components/schemas/User' },
                    },
                  },
                },
              },
            },
            400: { description: 'Bad request or validation error' },
          },
        },
      },
      '/api/auth/login': {
        post: {
          tags: ['Authentication'],
          summary: 'User Session Authentication Login',
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  required: ['email', 'password'],
                  properties: {
                    email: { type: 'string', format: 'email', example: 'user@example.com' },
                    password: { type: 'string', format: 'password', example: 'Password123!' },
                  },
                },
              },
            },
          },
          responses: {
            200: {
              description: 'Successfully authenticated session',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      token: { type: 'string' },
                      user: { $ref: '#/components/schemas/User' },
                    },
                  },
                },
              },
            },
            401: { description: 'Invalid email or password' },
          },
        },
      },
      '/api/auth/profile': {
        get: {
          tags: ['Authentication'],
          summary: 'Retrieve own profile details',
          security: [{ BearerAuth: [] }],
          responses: {
            200: {
              description: 'Profile details',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      user: { $ref: '#/components/schemas/User' },
                      preferences: { type: 'object' },
                    },
                  },
                },
              },
            },
            401: { description: 'Unauthorized session' },
          },
        },
        put: {
          tags: ['Authentication'],
          summary: 'Update own profile details',
          security: [{ BearerAuth: [] }],
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  properties: {
                    fullName: { type: 'string', example: 'Johnathan Doe' },
                    password: { type: 'string', format: 'password', example: 'NewSecret123!' },
                  },
                },
              },
            },
          },
          responses: {
            200: { description: 'Profile updated successfully' },
            400: { description: 'Validation error' },
            401: { description: 'Unauthorized session' },
          },
        },
      },
      '/api/auth/forgot-password': {
        post: {
          tags: ['Authentication'],
          summary: 'Trigger forgotten password reset email flow',
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  required: ['email'],
                  properties: {
                    email: { type: 'string', format: 'email', example: 'user@example.com' },
                  },
                },
              },
            },
          },
          responses: {
            200: { description: 'Reset token successfully generated' },
          },
        },
      },
      '/api/auth/reset-password': {
        post: {
          tags: ['Authentication'],
          summary: 'Validate token and reset password',
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  required: ['resetToken', 'password'],
                  properties: {
                    resetToken: { type: 'string', example: 'token_string' },
                    password: { type: 'string', format: 'password', example: 'NewStrongPassword1!' },
                  },
                },
              },
            },
          },
          responses: {
            200: { description: 'Password reset successfully' },
            400: { description: 'Invalid or expired token' },
          },
        },
      },
      '/api/auth/verify-email': {
        post: {
          tags: ['Authentication'],
          summary: 'Confirm verification token',
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  required: ['token'],
                  properties: {
                    token: { type: 'string', example: 'verification_token' },
                  },
                },
              },
            },
          },
          responses: {
            200: { description: 'Email address verified successfully' },
            400: { description: 'Invalid verification token' },
          },
        },
      },
      '/api/auth/preferences': {
        put: {
          tags: ['Authentication'],
          summary: 'Update UI preferences',
          security: [{ BearerAuth: [] }],
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  properties: {
                    theme: { type: 'string', example: 'dark' },
                    timezone: { type: 'string', example: 'Asia/Kolkata' },
                  },
                },
              },
            },
          },
          responses: {
            200: { description: 'Preferences updated successfully' },
          },
        },
      },
      '/api/auth/sessions': {
        get: {
          tags: ['Authentication'],
          summary: 'Retrieve logged-in active sessions',
          security: [{ BearerAuth: [] }],
          responses: {
            200: {
              description: 'Active sessions array',
              content: {
                'application/json': {
                  schema: {
                    type: 'array',
                    items: {
                      type: 'object',
                      properties: {
                        deviceInfo: { type: 'string' },
                        ipAddress: { type: 'string' },
                        lastActive: { type: 'string', format: 'date-time' },
                        isCurrent: { type: 'boolean' },
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
      '/api/auth/logout-all': {
        post: {
          tags: ['Authentication'],
          summary: 'Log out from all other active device sessions',
          security: [{ BearerAuth: [] }],
          responses: {
            200: { description: 'Session list cleared except current' },
          },
        },
      },
      '/api/auth/account': {
        delete: {
          tags: ['Authentication'],
          summary: 'Delete account and related portfolio permanently',
          security: [{ BearerAuth: [] }],
          responses: {
            200: { description: 'Account and related assets successfully deleted' },
          },
        },
      },
      '/api/watchlists': {
        get: {
          tags: ['Watchlists'],
          summary: 'Retrieve watchlists',
          responses: {
            200: {
              description: 'List of watchlists',
              content: {
                'application/json': {
                  schema: {
                    type: 'array',
                    items: { $ref: '#/components/schemas/Watchlist' },
                  },
                },
              },
            },
          },
        },
        post: {
          tags: ['Watchlists'],
          summary: 'Create custom watchlist',
          security: [{ BearerAuth: [] }],
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  required: ['name'],
                  properties: {
                    name: { type: 'string', example: 'Techno Growth' },
                  },
                },
              },
            },
          },
          responses: {
            201: {
              description: 'Watchlist created successfully',
              content: {
                'application/json': {
                  schema: { $ref: '#/components/schemas/Watchlist' },
                },
              },
            },
            400: { description: 'Duplicate name or validation error' },
          },
        },
      },
      '/api/watchlists/{name}': {
        delete: {
          tags: ['Watchlists'],
          summary: 'Delete custom watchlist',
          security: [{ BearerAuth: [] }],
          parameters: [
            { name: 'name', in: 'path', required: true, schema: { type: 'string' } },
          ],
          responses: {
            200: { description: 'Watchlist deleted' },
            404: { description: 'Watchlist not found' },
          },
        },
        put: {
          tags: ['Watchlists'],
          summary: 'Rename watchlist',
          security: [{ BearerAuth: [] }],
          parameters: [
            { name: 'name', in: 'path', required: true, schema: { type: 'string' } },
          ],
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  required: ['name'],
                  properties: {
                    name: { type: 'string', example: 'Tech Titans' },
                  },
                },
              },
            },
          },
          responses: {
            200: { description: 'Watchlist renamed' },
          },
        },
      },
      '/api/stocks': {
        get: {
          tags: ['Stocks'],
          summary: 'Fetch all stocks for watchlist',
          parameters: [
            { name: 'watchlist', in: 'query', schema: { type: 'string', default: 'default' } },
          ],
          responses: {
            200: {
              description: 'Watchlist stocks',
              content: {
                'application/json': {
                  schema: { type: 'array', items: { $ref: '#/components/schemas/Stock' } },
                },
              },
            },
          },
        },
        post: {
          tags: ['Stocks'],
          summary: 'Add stock to watchlist',
          security: [{ BearerAuth: [] }],
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  required: ['symbol', 'name'],
                  properties: {
                    symbol: { type: 'string', example: 'TCS.NS' },
                    name: { type: 'string', example: 'Tata Consultancy Services Limited' },
                    isFavourite: { type: 'boolean', example: false },
                    watchlist: { type: 'string', example: 'default' },
                  },
                },
              },
            },
          },
          responses: {
            201: { description: 'Stock added' },
          },
        },
      },
      '/api/stocks/{symbol}': {
        patch: {
          tags: ['Stocks'],
          summary: 'Update stock favourite status or tags',
          security: [{ BearerAuth: [] }],
          parameters: [
            { name: 'symbol', in: 'path', required: true, schema: { type: 'string' } },
          ],
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  properties: {
                    watchlist: { type: 'string', example: 'default' },
                    isFavourite: { type: 'boolean', example: true },
                    tags: { type: 'array', items: { type: 'string' } },
                  },
                },
              },
            },
          },
          responses: {
            200: { description: 'Stock updated' },
          },
        },
        delete: {
          tags: ['Stocks'],
          summary: 'Delete stock and drawings',
          security: [{ BearerAuth: [] }],
          parameters: [
            { name: 'symbol', in: 'path', required: true, schema: { type: 'string' } },
            { name: 'watchlist', in: 'query', schema: { type: 'string', default: 'default' } },
          ],
          responses: {
            200: { description: 'Stock deleted' },
          },
        },
      },
      '/api/stocks/search': {
        get: {
          tags: ['Stocks'],
          summary: 'Server-side paginated stock search',
          parameters: [
            { name: 'q', in: 'query', schema: { type: 'string' } },
            { name: 'watchlist', in: 'query', schema: { type: 'string', default: 'default' } },
            { name: 'page', in: 'query', schema: { type: 'integer', default: 1 } },
            { name: 'limit', in: 'query', schema: { type: 'integer', default: 50 } },
          ],
          responses: {
            200: {
              description: 'Paginated results',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      stocks: { type: 'array', items: { $ref: '#/components/schemas/Stock' } },
                      total: { type: 'integer' },
                      page: { type: 'integer' },
                      totalPages: { type: 'integer' },
                    },
                  },
                },
              },
            },
          },
        },
      },
      '/api/custom-tags': {
        get: {
          tags: ['Custom Tags'],
          summary: 'Fetch custom tags config list',
          responses: {
            200: {
              description: 'Tags array',
              content: {
                'application/json': {
                  schema: { type: 'array', items: { $ref: '#/components/schemas/CustomTag' } },
                },
              },
            },
          },
        },
      },
      '/api/custom-tags/{tagId}': {
        put: {
          tags: ['Custom Tags'],
          summary: 'Update custom tag metadata details',
          parameters: [
            { name: 'tagId', in: 'path', required: true, schema: { type: 'string' } },
          ],
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  required: ['label', 'color'],
                  properties: {
                    label: { type: 'string', example: 'High Yield' },
                    color: { type: 'string', example: '#10b981' },
                  },
                },
              },
            },
          },
          responses: {
            200: { description: 'Tag updated' },
          },
        },
      },
      '/api/watchlists/bulk-add': {
        post: {
          tags: ['Watchlists'],
          summary: 'Add multiple stocks at once',
          security: [{ BearerAuth: [] }],
          requestBody: {
            required: true,
            content: {
              'application/json': {
                schema: {
                  type: 'object',
                  required: ['watchlist', 'stocks'],
                  properties: {
                    watchlist: { type: 'string', example: 'default' },
                    stocks: {
                      type: 'array',
                      items: {
                        type: 'object',
                        required: ['symbol', 'name'],
                        properties: {
                          symbol: { type: 'string', example: 'INFY.NS' },
                          name: { type: 'string', example: 'Infosys Limited' },
                        },
                      },
                    },
                  },
                },
              },
            },
          },
          responses: {
            201: { description: 'Bulk add successful' },
          },
        },
      },
      '/api/notifications': {
        get: {
          tags: ['Notifications'],
          summary: 'Fetch user notifications',
          security: [{ BearerAuth: [] }],
          parameters: [
            { name: 'limit', in: 'query', schema: { type: 'integer', default: 50 } },
          ],
          responses: {
            200: {
              description: 'Notifications list',
              content: {
                'application/json': {
                  schema: {
                    type: 'object',
                    properties: {
                      notifications: { type: 'array', items: { $ref: '#/components/schemas/Notification' } },
                      unreadCount: { type: 'integer' },
                    },
                  },
                },
              },
            },
          },
        },
      },
      '/api/notifications/{id}/read': {
        put: {
          tags: ['Notifications'],
          summary: 'Mark single notification as read',
          security: [{ BearerAuth: [] }],
          parameters: [
            { name: 'id', in: 'path', required: true, schema: { type: 'string' } },
          ],
          responses: {
            200: { description: 'Notification read' },
          },
        },
      },
      '/api/notifications/read-all': {
        put: {
          tags: ['Notifications'],
          summary: 'Mark all user notifications as read',
          security: [{ BearerAuth: [] }],
          responses: {
            200: { description: 'All notifications marked as read' },
          },
        },
      },
      '/api/notifications/{id}': {
        delete: {
          tags: ['Notifications'],
          summary: 'Delete notification',
          security: [{ BearerAuth: [] }],
          parameters: [
            { name: 'id', in: 'path', required: true, schema: { type: 'string' } },
          ],
          responses: {
            200: { description: 'Notification deleted' },
          },
        },
      },
      '/api/corporate-actions': {
        get: {
          tags: ['Corporate Actions'],
          summary: 'Get corporate actions feed',
          description: 'Retrieve dynamic corporate actions based on user holdings and watchlists.',
          responses: {
            200: {
              description: 'List of corporate actions',
              content: {
                'application/json': {
                  schema: { type: 'array', items: { $ref: '#/components/schemas/CorporateAction' } },
                },
              },
            },
          },
        },
      },
    },
    security: [
      {
        BearerAuth: [],
      },
    ],
  },
  apis: ['./server.js', './routes/*.js'],
};

const specs = swaggerJsdoc(options);

// Gorgeous Dark/Light Adaptive Premium Theme Styling for Swagger UI
const customCss = `
  /* Clean Swagger Branding */
  .swagger-ui .topbar { display: none }
  
  /* Modern CSS Variable system */
  :root {
    --bg-base: #ffffff;
    --bg-surface: #f8fafc;
    --border-color: #e2e8f0;
    --text-primary: #0f172a;
    --text-secondary: #475569;
  }

  /* Automatically switch colors matching current system preference */
  @media (prefers-color-scheme: dark) {
    :root {
      --bg-base: #0b0f19;
      --bg-surface: #111827;
      --border-color: #1f2937;
      --text-primary: #f8fafc;
      --text-secondary: #94a3b8;
    }

    body {
      background-color: var(--bg-base) !important;
    }

    .swagger-ui {
      background-color: var(--bg-base) !important;
      color: var(--text-primary) !important;
      filter: invert(0) !important;
    }

    .swagger-ui .info .title,
    .swagger-ui .info li,
    .swagger-ui .info p,
    .swagger-ui .info a,
    .swagger-ui .info table,
    .swagger-ui .scheme-container,
    .swagger-ui select,
    .swagger-ui label,
    .swagger-ui .dialog-ux .modal-ux,
    .swagger-ui .dialog-ux .modal-ux-content p,
    .swagger-ui .dialog-ux .modal-ux-header h3,
    .swagger-ui .btn.authorize,
    .swagger-ui .opblock .opblock-summary-path,
    .swagger-ui .opblock .opblock-summary-description,
    .swagger-ui .opblock-tag,
    .swagger-ui .tab li button.tablinks,
    .swagger-ui .response-col_status,
    .swagger-ui .response-col_description,
    .swagger-ui table thead tr td, 
    .swagger-ui table thead tr th,
    .swagger-ui .parameter__name,
    .swagger-ui .parameter__type,
    .swagger-ui .parameter__in,
    .swagger-ui .model-title,
    .swagger-ui .model {
      color: var(--text-primary) !important;
    }

    .swagger-ui .scheme-container,
    .swagger-ui .opblock,
    .swagger-ui select,
    .swagger-ui input[type=text],
    .swagger-ui textarea,
    .swagger-ui .dialog-ux .modal-ux {
      background-color: var(--bg-surface) !important;
      border-color: var(--border-color) !important;
      color: var(--text-primary) !important;
    }

    .swagger-ui .opblock .opblock-section-header,
    .swagger-ui .opblock .opblock-summary {
      border-bottom-color: var(--border-color) !important;
    }

    .swagger-ui .btn.authorize {
      border-color: #10b981 !important;
      background-color: rgba(16, 185, 129, 0.1) !important;
      color: #10b981 !important;
    }

    .swagger-ui .btn.authorize svg {
      fill: #10b981 !important;
    }
  }
`;

function setupSwagger(app) {
  app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(specs, {
    customCss: customCss,
    customJs: '/swagger-custom.js',
    customSiteTitle: "Vision API Documentation",
  }));
  console.log('Swagger UI registered at http://localhost:5001/api-docs');
}

module.exports = {
  setupSwagger,
};
