const { Client, GatewayIntentBits, SlashCommandBuilder, EmbedBuilder, REST, Routes } = require('discord.js');
const axios = require('axios');
const fs = require('fs').promises;
const path = require('path');
const Bottleneck = require('bottleneck');
require('dotenv').config();

class HiveSnipingBot {
    constructor() {
        this.client = new Client({
            intents: [GatewayIntentBits.Guilds]
        });

        this.dataFile = path.join(__dirname, 'bot_data.json');
        this.trackedUsers = new Map();
        this.tempUsers = new Map();
        this.pingSubscribers = new Map();
        this.winstreaks = new Map();
        this.lastSeen = new Map();
        this.lastWin = new Map();
        this.matchTimes = new Map();
        this.hotPlayerAlerts = new Map();
        this.queuePredictionMessages = new Map();
        this.lastSeenMessageId = null;
        this.lastSeenUpdateInterval = null;
        this.onlyViewUsers = new Set();
        this.dmSubscribers = new Map();
        this.inactiveUserLastCheck = new Map();
        this.dailyStats = new Map();
        this.gamemodeDetector = new GamemodeDetector();  // ← KLEIN GESCHRIEBEN!
        this.adminIds = (process.env.ADMIN_IDS || '').split(',').filter(id => id);
        
        this.pollInterval = 5000;
        this.tempUserCheckInterval = 300000;
        this.lastSeenUpdateFrequency = 60000;
        this.isPolling = false

        //Rate Limiter
        this.limiter = new Bottleneck({
            maxConcurrent: 10,
            minTime: 0,
            reservoir: 120,
            reservoirRefreshAmount: 120,
            reservoirRefreshInterval: 60 * 1000,
        });

        //Limiter fail Log
        this.limiter.on('failed', (error, info) => {
            console.error('❌ Limiter job failed:', error.message);
        });

        this.gamemodeNames = {
            bed: 'BedWars',
            dr: 'Death Run',
            hide: 'Hide and Seek',
            party: 'Block Party',
            drop: 'Block Drop',
            ground: 'Ground Wars',
            sky: 'Sky Wars',
            ctf: 'Capture The Flag',
            bridge: 'The Bridge',
            murder: 'Murder Mystery',
            sg: 'Survival Games'
        };

        this.init();
    }

    startDailyStatsReset() {
        const getNextMidnightCET = () => {
            const now = new Date();
            const cetTime = new Date(now.toLocaleString('en-US', { timeZone: 'Europe/Berlin' }));
            const tomorrow = new Date(cetTime);
            tomorrow.setDate(tomorrow.getDate() + 1);
            tomorrow.setHours(0, 0, 0, 0);
            const diff = tomorrow - cetTime;
            return diff;
        };
    
        const scheduleReset = () => {
            const msUntilMidnight = getNextMidnightCET();
            console.log(`Daily stats will reset in ${Math.floor(msUntilMidnight / 1000 / 60)} minutes`);
            
            setTimeout(() => {
                this.resetDailyStats();
                scheduleReset();
            }, msUntilMidnight);
        };

        scheduleReset();
    }

    resetDailyStats() {
        console.log('Resetting daily stats for all users (midnight CET)');
        const today = new Date().toLocaleDateString('de-DE', { timeZone: 'Europe/Berlin' });
        
        for (const [username, userStats] of this.dailyStats.entries()) {
            for (const gamemode of Object.keys(userStats)) {
                userStats[gamemode] = {
                kills: 0,
                deaths: 0,
                lastResetDate: today
            };
        }
    }
        this.saveBotData();
        console.log(`Daily stats reset complete for ${this.dailyStats.size} users`);
    }

    updateDailyStats(username, gamemode, kills, deaths) {
    try {
        const normalizedUsername = username.toLowerCase();
        const today = new Date().toLocaleDateString('de-DE', { timeZone: 'Europe/Berlin' });
        
        // Initialisiere User wenn nicht vorhanden
        if (!this.dailyStats.has(normalizedUsername)) {
            this.dailyStats.set(normalizedUsername, {});
        }
        
        const userStats = this.dailyStats.get(normalizedUsername);
        
        // Sicherheitscheck für userStats
        if (!userStats || typeof userStats !== 'object') {
            console.error(`Daily stats for ${normalizedUsername} is invalid, reinitializing`);
            this.dailyStats.set(normalizedUsername, {});
            const newUserStats = this.dailyStats.get(normalizedUsername);
            newUserStats[gamemode] = {
                kills: kills,
                deaths: deaths,
                lastResetDate: today
            };
            return;
        }
        
        // Initialisiere Gamemode wenn nicht vorhanden ODER wenn es kein gültiges Objekt ist
        if (!userStats[gamemode] || typeof userStats[gamemode] !== 'object' || !userStats[gamemode].lastResetDate) {
            userStats[gamemode] = {
                kills: 0,
                deaths: 0,
                lastResetDate: today
            };
        }
        
        // Jetzt sicher auf gamemodeStats zugreifen
        const gamemodeStats = userStats[gamemode];
        
        // Reset wenn neuer Tag
        if (gamemodeStats.lastResetDate !== today) {
            gamemodeStats.kills = 0;
            gamemodeStats.deaths = 0;
            gamemodeStats.lastResetDate = today;
        }
        
        // Update Stats
        gamemodeStats.kills = (gamemodeStats.kills || 0) + kills;
        gamemodeStats.deaths = (gamemodeStats.deaths || 0) + deaths;
        
        // Setze die aktualisierten Stats zurück in die Map
        this.dailyStats.set(normalizedUsername, userStats);
        
    } catch (error) {
        console.error(`Error updating daily stats for ${username} in ${gamemode}:`, error);
        // Notfall: Initialisiere komplett neu
        const today = new Date().toLocaleDateString('de-DE', { timeZone: 'Europe/Berlin' });
        this.dailyStats.set(username.toLowerCase(), {
            [gamemode]: {
                kills: kills,
                deaths: deaths,
                lastResetDate: today
            }
        });
    }
 }   

    getDailyKD(username, gamemode) {
    const normalizedUsername = username.toLowerCase();
    const userStats = this.dailyStats.get(normalizedUsername);
    
    // Check ob user stats existieren
    if (!userStats || typeof userStats !== 'object') {
        return null;
    }
    
    // Check ob gamemode stats existieren
    if (!userStats[gamemode] || typeof userStats[gamemode] !== 'object') {
        return null;
    }
    
    const gamemodeStats = userStats[gamemode];
    
    // Check ob kills existiert
    if (typeof gamemodeStats.kills !== 'number' || gamemodeStats.kills === 0) {
        return null;
    }
    
    // Check ob deaths existiert
    if (typeof gamemodeStats.deaths !== 'number' || gamemodeStats.deaths === 0) {
        return gamemodeStats.kills.toFixed(2);
    }
    
    return (gamemodeStats.kills / gamemodeStats.deaths).toFixed(2);
 }


    async init() {
        await this.loadBotData();
        await this.setupCommands();

        this.client.once('ready', () => {
            console.log(`Bot is ready! Logged in as ${this.client.user.tag}`);
            this.startPolling();
            this.startTempUserCleanup();
            this.initializeLastSeenEmbed();
            this.startLastSeenUpdater();
            this.startDailyStatsReset();
        });

        this.client.on('interactionCreate', async (interaction) => {
            if (!interaction.isCommand()) return;
            await this.handleCommand(interaction);
        });

        await this.client.login(process.env.DISCORD_TOKEN);
    }


    async setupCommands() {
        const commands = [
            new SlashCommandBuilder()
                .setName('adduser')
                .setDescription('Add a player to track permanently')
                .addStringOption(option =>
                    option.setName('username')
                        .setDescription('Hive username to track')
                        .setRequired(true)
                ),
            new SlashCommandBuilder()
                .setName('addtempuser')
                .setDescription('Add a player to track temporarily')
                .addStringOption(option =>
                    option.setName('username')
                        .setDescription('Hive username to track')
                        .setRequired(true)
                )
                .addStringOption(option =>
                    option.setName('duration')
                        .setDescription('How long to track (1d, 3d, 7d)')
                        .setRequired(true)
                        .addChoices(
                            { name: '1 day', value: '1d' },
                            { name: '3 days', value: '3d' },
                            { name: '7 days', value: '7d' }
                        )
                ),
            new SlashCommandBuilder()
                .setName('removeuser')
                .setDescription('Remove a player from tracking')
                .addStringOption(option =>
                    option.setName('username')
                        .setDescription('Username to stop tracking')
                        .setRequired(true)
                ),
            new SlashCommandBuilder()
                .setName('listusers')
                .setDescription('List all tracked players'),
            new SlashCommandBuilder()
                .setName('addping')
                .setDescription('Get notified when a player wins')
                .addStringOption(option =>
                    option.setName('username')
                        .setDescription('Player to get notifications for')
                        .setRequired(true)
                ),
            new SlashCommandBuilder()
                .setName('removeping')
                .setDescription('Stop getting notifications for a player')
                .addStringOption(option =>
                    option.setName('username')
                        .setDescription('Player to stop notifications for')
                        .setRequired(true)
                ),
            new SlashCommandBuilder()
                .setName('customrate')
                .setDescription('Set custom winstreak for a player (Admin only)')
                .addStringOption(option =>
                    option.setName('username')
                        .setDescription('Player username')
                        .setRequired(true)
                )
                .addStringOption(option =>
                    option.setName('gamemode')
                        .setDescription('Gamemode')
                        .setRequired(true)
                )
                .addIntegerOption(option =>
                    option.setName('value')
                        .setDescription('Winstreak value')
                        .setRequired(true)
                ),
            new SlashCommandBuilder()
                .setName('onlyview')
                .setDescription('Set a user to only view (Admin only)')
                .addStringOption(option =>
                    option.setName('userid')
                        .setDescription('Discord User ID to set as only-view')
                        .setRequired(true)
                )
                .addStringOption(option =>
                    option.setName('action')
                        .setDescription('Add or remove from only-view')
                        .setRequired(true)
                        .addChoices(
                            { name: 'Add', value: 'add' },
                            { name: 'Remove', value: 'remove' }
                        )
                ),
            new SlashCommandBuilder()
                .setName('dm')
                .setDescription('Setup private DM notifications for user (Admin Only)')
                .addStringOption(option =>
                    option.setName('userid')
                       .setDescription('Discord User ID to receive DMs')
                       .setRequired(true)
                )
            .addStringOption(option =>
                option.setName('username')
                    .setDescription('Player username to track via DM')
                    .setRequired(true)
                )
            .addStringOption(option =>
                option.setName('action')
                    .setDescription('Add or Remove DM subcription')
                    .setRequired(true)
                    .addChoices(
                       { name: 'Add', value: 'add'},
                       { name: 'Remove', value: 'remove'}
                    )
            ),

        ];

        const rest = new REST().setToken(process.env.DISCORD_TOKEN);

        try {
            console.log('Refreshing slash commands...');
            await rest.put(
                Routes.applicationCommands(process.env.CLIENT_ID),
                { body: commands.map(cmd => cmd.toJSON()) }
            );
            console.log('Slash commands registered successfully');
        } catch (error) {
            console.error('Error registering slash commands:', error);
        }
    }

    async handleCommand(interaction) {
        const { commandName, options } = interaction;

        try {
            switch (commandName) {
                case 'adduser':
                    await this.handleAddUser(interaction, options.getString('username'));
                    break;
                case 'addtempuser':
                    await this.handleAddTempUser(interaction, options.getString('username'), options.getString('duration'));
                    break;
                case 'removeuser':
                    await this.handleRemoveUser(interaction, options.getString('username'));
                    break;
                case 'listusers':
                    await this.handleListUsers(interaction);
                    break;
                case 'addping':
                    await this.handleAddPing(interaction, options.getString('username'));
                    break;
                case 'removeping':
                    await this.handleRemovePing(interaction, options.getString('username'));
                    break;
                case 'customrate':
                    await this.handleCustomRate(interaction, options.getString('username'), options.getString('gamemode'), options.getInteger('value'));
                    break;
                 case 'onlyview':
                    await this.handleOnlyView(interaction, options.getString('userid'), options.getString('action'));
                    break;
                case 'dm':
                    await this.handleDMCommand(interaction, options.getString('userid'), options.getString('username'), options.getString('action'));
                    break;
            }
        } catch (error) {
            console.error('Command error:', error);
            const errorMessage = `Error: ${error.message || error}`;
            
            if (interaction.replied || interaction.deferred) {
                await interaction.followUp({ content: errorMessage, flags: 64 });
            } else {
                await interaction.editReply({ content: errorMessage, flags: 64 });
            }
        }
    }

    async handleCustomRate(interaction, username, gamemode, value) {
        await interaction.deferReply({ flags: 64 });
        // Check if user is admin
        if (!this.adminIds.includes(interaction.user.id)) {
            return await interaction.editReply({
                content: 'You are not permitted to use this command.',
                flags: 64
            });
        }

        if (!username || !gamemode || value === null) {
            return await interaction.editReply({ 
                content: 'Please provide valid username, gamemode, and value.', 
                flags: 64 
            });
        }

        const normalizedUsername = username.toLowerCase();
        
        // Check if user is being tracked
        if (!this.trackedUsers.has(normalizedUsername) && !this.tempUsers.has(normalizedUsername)) {
            return await interaction.editReply({ 
                content: `**${username}** is not currently being tracked.`, 
                flags: 64 
            });
        }

        // Initialize winstreaks for this user if not exists
        if (!this.winstreaks.has(normalizedUsername)) {
            this.winstreaks.set(normalizedUsername, {});
        }

        const userWinstreaks = this.winstreaks.get(normalizedUsername);
        userWinstreaks[gamemode.toLowerCase()] = value;

        await this.saveBotData();

        const actualUsername = this.getActualUsername(normalizedUsername);
        const gamemodeDisplay = this.gamemodeNames[gamemode.toLowerCase()] || gamemode.toUpperCase();

        const embed = new EmbedBuilder()
            .setColor(0x00ff00)
            .setTitle('Custom Rate Set')
            .setDescription(`Set **${actualUsername}**'s ${gamemodeDisplay} winstreak to **${value}**`)
            .addFields({
                name: 'Updated by',
                value: interaction.user.tag,
                inline: true
            })
            .setTimestamp();

        await interaction.editReply({ embeds: [embed] });
        console.log(`${interaction.user.tag} set ${actualUsername}'s ${gamemode} winstreak to ${value}`);
    }

    async handleOnlyView(interaction, userId, action) {
        await interaction.deferReply({ flags: 64 });
        // Check if user is admin
        if (!this.adminIds.includes(interaction.user.id)) {
            return await interaction.editReply({
                content: 'You are not permitted to use this command.',
                flags: 64
            });
        }

        if (!userId || !action) {
            return await interaction.editReply({ 
                content: 'Please provide valid user ID and action.', 
                flags: 64 
            });
        }

        // Validate user ID format (Discord IDs are 17-21 digits)
        if (!/^\d{17,21}$/.test(userId)) {
            return await interaction.editReply({ 
                content: 'Please provide a valid Discord User ID.', 
                flags: 64 
            });
        }

        let message = '';
        let color = 0x00ff00;

        if (action === 'add') {
            if (this.onlyViewUsers.has(userId)) {
                return await interaction.editReply({ 
                    content: `User <@${userId}> is already set to only-view.`, 
                    flags: 64 
                });
            }
            
            this.onlyViewUsers.add(userId);
            message = `User <@${userId}> has been set to only-view mode.`;
            color = 0xffa500;
        } else if (action === 'remove') {
            if (!this.onlyViewUsers.has(userId)) {
                return await interaction.editReply({ 
                    content: `User <@${userId}> is not in only-view mode.`, 
                    flags: 64
                });
            }
            
            this.onlyViewUsers.delete(userId);
            message = `User <@${userId}> has been removed from only-view mode.`;
            color = 0x00ff00;
        }

        await this.saveBotData();

        // Send logging message
        await this.sendLogMessage('onlyview', interaction.user.id, `<@${userId}>`, action);

        const embed = new EmbedBuilder()
            .setColor(color)
            .setTitle('Only-View Status Updated')
            .setDescription(message)
            .addFields({
                name: 'Updated by',
                value: interaction.user.tag,
                inline: true
            })
            .setTimestamp();

        await interaction.editReply({ embeds: [embed] });
        console.log(`${interaction.user.tag} set ${userId} to only-view: ${action}`);
    }

    async handleDMCommand(interaction, userId, username, action) {
        await interaction.deferReply({ flags: 64 });
        //check if user is Admin
        if (!this.adminIds.includes(interaction.user.id)) {
            return await interaction.editReply({
                content: 'You are not permitted to use this command.',
                flags: 64
            });
        }

        if (!userId || !username || !action) {
        return await interaction.editReply({ 
            content: 'Please provide valid user ID, username, and action.', 
            flags: 64 
        });
    }

    // Validate user ID format
    if (!/^\d{17,21}$/.test(userId)) {
        return await interaction.editReply({ 
            content: 'Please provide a valid Discord User ID.', 
            flags: 64
        });
    }

    const normalizedUsername = username.toLowerCase();
    
    // Check if player is being tracked
    if (!this.trackedUsers.has(normalizedUsername) && !this.tempUsers.has(normalizedUsername)) {
        return await interaction.editReply({ 
            content: `**${username}** is not currently being tracked. Add them first with \`/adduser\` or \`/addtempuser\`.`, 
            flags: 64
        });
    }

    // Initialize DM subscribers for this user if not exists
    if (!this.dmSubscribers.has(userId)) {
        this.dmSubscribers.set(userId, new Set());
    }

    const userDMSubscriptions = this.dmSubscribers.get(userId);
    let message = '';
    let color = 0x00ff00;

    if (action === 'add') {
        if (userDMSubscriptions.has(normalizedUsername)) {
            return await interaction.editReply({ 
                content: `User <@${userId}> is already receiving DMs for **${username}**.`, 
                flags: 64 
            });
        }
        
        userDMSubscriptions.add(normalizedUsername);
        message = `User <@${userId}> will now receive DMs when **${username}** wins/loses games.`;
        color = 0x00ff00;
    } else if (action === 'remove') {
        if (!userDMSubscriptions.has(normalizedUsername)) {
            return await interaction.editReply({ 
                content: `User <@${userId}> is not receiving DMs for **${username}**.`, 
                flags: 64 
            });
        }
        
        userDMSubscriptions.delete(normalizedUsername);
        message = `User <@${userId}> will no longer receive DMs for **${username}**.`;
        color = 0xff6b6b;
        
        // Clean up empty subscriptions
        if (userDMSubscriptions.size === 0) {
            this.dmSubscribers.delete(userId);
        }
    }

    await this.saveBotData();

    // Send logging message
    await this.sendLogMessage('dm', interaction.user.id, `<@${userId}> for **${username}**`, action);

    const embed = new EmbedBuilder()
        .setColor(color)
        .setTitle('DM Subscription Updated')
        .setDescription(message)
        .addFields(
            {
                name: 'Updated by',
                value: interaction.user.tag,
                inline: true
            },
            {
                name: 'Total DM subscriptions for user',
                value: userDMSubscriptions.size.toString(),
                inline: true
            }
        )
        .setTimestamp();

    await interaction.editReply({ embeds: [embed] });
    console.log(`${interaction.user.tag} ${action}ed DM subscription: ${userId} for ${username}`);
    }



    async sendLogMessage(command, executorId, affectedUser, additionalInfo = null) {
        try {
            const logChannel = this.client.channels.cache.find(channel => channel.name === 'sniping-logs');
            if (!logChannel) {
                console.warn('sniping-logs channel not found');
                return;
            }

            let description = `**Command:** \`/${command}\`\n`;
            description += `**Executed by:** <@${executorId}> (${executorId})\n`;
            description += `**Affected:** ${affectedUser}`;
            
            if (additionalInfo) {
                description += `\n**Additional Info:** ${additionalInfo}`;
            }

            const embed = new EmbedBuilder()
                .setColor(0x4a9eff)
                .setTitle('Command Executed')
                .setDescription(description)
                .setTimestamp();

            await logChannel.send({ embeds: [embed] });
        } catch (error) {
            console.error('Error sending log message:', error);
        }
    }

    async handleAddUser(interaction, username) {
        console.log('1. handleAddUser started');

        if (this.onlyViewUsers.has(interaction.user.id)) {
            console.log('3. User in only-view mode');
            return await interaction.editReply({
                content: 'You are in only-view mode and cannot execute commands.',
                flags: 64
            });
        }

        await interaction.deferReply({ flags: 64 });

        if (!username) {
            console.log('4. No username provided');
            return await interaction.editReply({ 
                content: 'Please provide a valid username.', 
            });
        }

        console.log('5. Username provided:', username);
        const normalizedUsername = username.toLowerCase();
        console.log('6. Normalized username:', normalizedUsername);
        
        // Check if user is already tracked (permanent or temp)
        if (this.trackedUsers.has(normalizedUsername) || this.tempUsers.has(normalizedUsername)) {
            console.log('7. User already tracked');
            return await interaction.editReply({ 
                content: `**${username}** is already being tracked.`, 
            });
        }
        console.log('8. User not tracked yet');

        // Check if we're at the 75 player limit
        const totalUsers = this.trackedUsers.size + this.tempUsers.size;
        console.log('9. Total users:', totalUsers);
        if (totalUsers >= 75) {
            console.log('10. Max limit reached');
            return await interaction.editReply({
                content: 'Maximum tracking limit reached (75 players). Remove a player first.',
            });
        }
        console.log('11. Under limit, proceeding');

        try {
            console.log('12. About to fetch player stats');
            const playerData = await this.limiter.schedule(async () => {
                console.log('→ Inside limiter callback');
                return await this.fetchPlayerStats(username)
            });
            console.log('13. Player data received:', playerData);
            console.log('14. Checking if playerData is valid');
            
            if (!playerData || !playerData.main) {
                 console.log('15. Invalid player data - player not found');
                return await interaction.editReply({ 
                    content: `Could not find player **${username}** on Hive. Please check the username.` 
                });
            }

            const gameStats = this.extractGameStats(playerData);
            
            // Add to tracked users (permanent)
            this.trackedUsers.set(normalizedUsername, {
                username: playerData.username || username,
                lastStats: gameStats,
                addedAt: new Date().toISOString(),
                type: 'permanent'
            });

            // Initialize winstreaks for each gamemode
            const userWinstreaks = {};
            for (const gamemode of Object.keys(gameStats)) {
                userWinstreaks[gamemode] = 0;
            }
            this.winstreaks.set(normalizedUsername, userWinstreaks);

            // Initialize last seen, last win, and match times (gamemode-specific)
            this.lastSeen.set(normalizedUsername, Date.now());
            this.lastWin.set(normalizedUsername, Date.now()); // Initialize as active
            const gamemodeMatchTimes = {};
            for (const gamemode of Object.keys(gameStats)) {
                gamemodeMatchTimes[gamemode] = [];
            }
            this.matchTimes.set(normalizedUsername, gamemodeMatchTimes);
            this.hotPlayerAlerts.set(normalizedUsername, {});

            await this.saveBotData();
            await this.updateLastSeenEmbed();

            const embed = new EmbedBuilder()
                .setColor(0x00ff00)
                .setTitle('Player Added (Permanent)')
                .setDescription(`**${playerData.username || username}** is now being tracked permanently!`)
                .addFields(
                    { name: 'Total Games Played', value: this.getTotalGames(gameStats).toString(), inline: true },
                    { name: 'Total Victories', value: this.getTotalVictories(gameStats).toString(), inline: true }
                )
                .setTimestamp();

            await interaction.editReply({ embeds: [embed] });
            await this.sendLogMessage('adduser', interaction.user.id, `**${playerData.username || username}**`);
            console.log(`Added permanent user: ${playerData.username || username}`);

        } catch (error) {
            console.error('Error adding user:', error);
            await interaction.editReply({ 
                content: `Failed to add **${username}**. The player might not exist or the API might be unavailable.` 
            });
        }
    }

    async handleAddTempUser(interaction, username, duration) {
        await interaction.deferReply({ flags: 64 });
        if (this.onlyViewUsers.has(interaction.user.id)) {
            return await interaction.editReply({
                content: 'You are in only-view mode and cannot execute commands.',
                flags: 64
            });
        }

        if (!username || !duration) {
            return await interaction.editReply({ 
                content: 'Please provide a valid username and duration.', 
                flags: 64
            });
        }

        const normalizedUsername = username.toLowerCase();
        
        // Check if user is already tracked (permanent or temp)
        if (this.trackedUsers.has(normalizedUsername) || this.tempUsers.has(normalizedUsername)) {
            return await interaction.editReply({ 
                content: `**${username}** is already being tracked.`, 
                flags: 64 
            });
        }

        // Check if we're at the 75 player limit
        const totalUsers = this.trackedUsers.size + this.tempUsers.size;
        if (totalUsers >= 75) {
            return await interaction.editReply({
                content: 'Maximum tracking limit reached (75 players). Remove a player first.',
                flags: 64
            });
        }

        try {
            const playerData = await this.limiter.schedule(async () => {
                console.log('→ Inside limiter callback');
                return await this.fetchPlayerStats(username)
            });
            
            if (!playerData) {
                return await interaction.editReply({ 
                    content: `Could not find player **${username}** on Hive. Please check the username.` 
                });
            }

            const gameStats = this.extractGameStats(playerData);
            
            // Calculate expiry time
            const durationMs = this.parseDuration(duration);
            const expiresAt = new Date(Date.now() + durationMs);
            
            // Add to temp users
            this.tempUsers.set(normalizedUsername, {
                username: playerData.username || username,
                lastStats: gameStats,
                addedAt: new Date().toISOString(),
                expiresAt: expiresAt.toISOString(),
                type: 'temporary'
            });

            // Initialize winstreaks for each gamemode
            const userWinstreaks = {};
            for (const gamemode of Object.keys(gameStats)) {
                userWinstreaks[gamemode] = 0;
            }
            this.winstreaks.set(normalizedUsername, userWinstreaks);

            // Initialize last seen, last win, and match times (gamemode-specific)
            this.lastSeen.set(normalizedUsername, Date.now());
            this.lastWin.set(normalizedUsername, Date.now()); // Initialize as active
            const gamemodeMatchTimes = {};
            for (const gamemode of Object.keys(gameStats)) {
                gamemodeMatchTimes[gamemode] = [];
            }
            this.matchTimes.set(normalizedUsername, gamemodeMatchTimes);
            this.hotPlayerAlerts.set(normalizedUsername, {});

            await this.saveBotData();
            await this.updateLastSeenEmbed();

            const embed = new EmbedBuilder()
                .setColor(0xffa500)
                .setTitle('Temporary Player Added')
                .setDescription(`**${playerData.username || username}** is now being tracked temporarily!`)
                .addFields(
                    { name: 'Duration', value: this.formatDuration(duration), inline: true },
                    { name: 'Expires At', value: `<t:${Math.floor(expiresAt.getTime() / 1000)}:R>`, inline: true },
                    { name: 'Total Games', value: this.getTotalGames(gameStats).toString(), inline: true },
                    { name: 'Total Victories', value: this.getTotalVictories(gameStats).toString(), inline: true }
                )
                .setTimestamp();

            await interaction.editReply({ embeds: [embed] });
            await this.sendLogMessage('addtempuser', interaction.user.id, `**${playerData.username || username}**`, `Duration: ${this.formatDuration(duration)}`);
            console.log(`Added temporary user: ${playerData.username || username} (expires: ${expiresAt.toISOString()})`);

        } catch (error) {
            console.error('Error adding temp user:', error);
            await interaction.editReply({ 
                content: `Failed to add **${username}**. The player might not exist or the API might be unavailable.` 
            });
        }
    }

    async handleRemoveUser(interaction, username) {
        await interaction.deferReply({ flags: 64 });
        if (this.onlyViewUsers.has(interaction.user.id)) {
            return await interaction.editReply({
                content: 'You are in only-view mode and cannot execute commands.',
            });
        }

        if (!username) {
            return await interaction.editReply({ 
                content: 'Please provide a valid username.',  
            });
        }

        const normalizedUsername = username.toLowerCase();
        let userData = null;
        let wasTemp = false;
        
        if (this.trackedUsers.has(normalizedUsername)) {
            userData = this.trackedUsers.get(normalizedUsername);
            this.trackedUsers.delete(normalizedUsername);
        } else if (this.tempUsers.has(normalizedUsername)) {
            userData = this.tempUsers.get(normalizedUsername);
            this.tempUsers.delete(normalizedUsername);
            wasTemp = true;
        } else {
            return await interaction.editReply({ 
                content: `**${username}** is not currently being tracked.`, 
                flags: 64
            });
        }

        // Remove all related data
        this.winstreaks.delete(normalizedUsername);
        this.pingSubscribers.delete(normalizedUsername);
        this.lastSeen.delete(normalizedUsername);
        this.lastWin.delete(normalizedUsername);
        this.matchTimes.delete(normalizedUsername);
        this.hotPlayerAlerts.delete(normalizedUsername);
        this.inactiveUserLastCheck.delete(normalizedUsername);
        this.gamemodeDetector.clearHistory(normalizedUsername);
        this.dailyStats.delete(normalizedUsername);
        
        for (const [userId, subscribedUsernames] of this.dmSubscribers.entries()) {
        if (subscribedUsernames.has(normalizedUsername)) {
        subscribedUsernames.delete(normalizedUsername);
        if (subscribedUsernames.size === 0) {
            this.dmSubscribers.delete(userId);
        }
      }
    }
        
        await this.saveBotData();
        await this.updateLastSeenEmbed();

        const embed = new EmbedBuilder()
            .setColor(0xff6b6b)
            .setTitle('Player Removed')
            .setDescription(`**${userData.username}** has been removed from tracking.`)
            .addFields({
                name: 'Type',
                value: wasTemp ? 'Temporary' : 'Permanent',
                inline: true
            })
            .setTimestamp();

        await interaction.editReply({ embeds: [embed] });
        await this.sendLogMessage('removeuser', interaction.user.id, `**${userData.username}**`, `Type: ${wasTemp ? 'Temporary' : 'Permanent'}`);
        console.log(`Removed user: ${userData.username} (${wasTemp ? 'temp' : 'permanent'})`);
    }

    async handleListUsers(interaction) {
          await interaction.deferReply({ flags: 64 });
        const totalUsers = this.trackedUsers.size + this.tempUsers.size;
        
        if (totalUsers === 0) {
            return await interaction.editReply({ 
                content: 'No players are currently being tracked. Use `/adduser` or `/addtempuser` to start tracking someone!', 
                flags: 64
            });
        }

        let description = '';
        let index = 1;

        // Add permanent users
        if (this.onlyViewUsers.has(interaction.user.id)) {
            return await interaction.editReply({
                content: 'You are in only-view mode and cannot execute commands.',
                flags: 64
            });
        }

        if (this.trackedUsers.size > 0) {
            description += '**Permanent Users:**\n';
            for (const userData of this.trackedUsers.values()) {
                const totalGames = this.getTotalGames(userData.lastStats);
                const totalVictories = this.getTotalVictories(userData.lastStats);
                
                // Get all winstreaks for this user
                const userWinstreaks = this.winstreaks.get(userData.username.toLowerCase()) || {};
                const winstreakText = Object.entries(userWinstreaks)
                    .filter(([_, streak]) => streak > 0)
                    .map(([gamemode, streak]) => `${gamemode}:${streak}`)
                    .join(', ') || 'none';
                
                description += `**${index}.** ${userData.username} - ${totalGames} games, ${totalVictories} wins\n`;
                description += `    Winstreaks: ${winstreakText}\n`;
                index++;
            }
            description += '\n';
        }

        // Add temporary users
        if (this.onlyViewUsers.has(interaction.user.id)) {
            return await interaction.editReply({
                content: 'You are in only-view mode and cannot execute commands.',
                flags: 64
            });
        }

        if (this.tempUsers.size > 0) {
            description += '**Temporary Users:**\n';
            for (const userData of this.tempUsers.values()) {
                const totalGames = this.getTotalGames(userData.lastStats);
                const totalVictories = this.getTotalVictories(userData.lastStats);
                
                // Get all winstreaks for this user
                const userWinstreaks = this.winstreaks.get(userData.username.toLowerCase()) || {};
                const winstreakText = Object.entries(userWinstreaks)
                    .filter(([_, streak]) => streak > 0)
                    .map(([gamemode, streak]) => `${gamemode}:${streak}`)
                    .join(', ') || 'none';
                
                const expiresAt = Math.floor(new Date(userData.expiresAt).getTime() / 1000);
                description += `**${index}.** ${userData.username} - ${totalGames} games, ${totalVictories} wins (expires <t:${expiresAt}:R>)\n`;
                description += `    Winstreaks: ${winstreakText}\n`;
                index++;
            }
        }

        const embed = new EmbedBuilder()
            .setColor(0x4a9eff)
            .setTitle('Tracked Players')
            .setDescription(description)
            .addFields({
                name: 'Status',
                value: `Tracking **${totalUsers}/75** player${totalUsers !== 1 ? 's' : ''} (${this.trackedUsers.size} permanent, ${this.tempUsers.size} temporary)`,
                inline: true
            })
            .setFooter({ text: `Checking every ${this.pollInterval / 1000} seconds` })
            .setTimestamp();

        await interaction.editReply({ embeds: [embed] });
    }

    async handleAddPing(interaction, username) {
        await interaction.deferReply({ flags: 64 });
        if (!username) {
            return await interaction.editReply({ 
                content: 'Please provide a valid username.', 
                flags: 64
            });
        }

        const normalizedUsername = username.toLowerCase();
        const userId = interaction.user.id;

        // Check if user is being tracked
        if (!this.trackedUsers.has(normalizedUsername) && !this.tempUsers.has(normalizedUsername)) {
            return await interaction.editReply({ 
                content: `**${username}** is not currently being tracked. Add them first with \`/adduser\` or \`/addtempuser\`.`, 
                flags: 64
            });
        }

        // Initialize ping subscribers for this user if not exists
        if (!this.pingSubscribers.has(normalizedUsername)) {
            this.pingSubscribers.set(normalizedUsername, new Set());
        }

        const subscribers = this.pingSubscribers.get(normalizedUsername);
        
        if (subscribers.has(userId)) {
            return await interaction.editReply({ 
                content: `You're already subscribed to notifications for **${username}**.`, 
                flags: 64
            });
        }

        subscribers.add(userId);
        await this.saveBotData();

        const actualUsername = this.getActualUsername(normalizedUsername);

        const embed = new EmbedBuilder()
            .setColor(0x00ff00)
            .setTitle('Notification Added')
            .setDescription(`You will now be pinged when **${actualUsername}** wins a game!`)
            .addFields({
                name: 'Total Subscribers',
                value: subscribers.size.toString(),
                inline: true
            })
            .setTimestamp();

        await interaction.editReply({ embeds: [embed] });
        console.log(`${interaction.user.tag} subscribed to notifications for ${actualUsername}`);
    }

    async handleRemovePing(interaction, username) {
        await interaction.deferReply({ flags: 64 });
        if (!username) {
            return await interaction.editReply({ 
                content: 'Please provide a valid username.', 
                flags: 64
            });
        }

        const normalizedUsername = username.toLowerCase();
        const userId = interaction.user.id;

        if (!this.pingSubscribers.has(normalizedUsername)) {
            return await interaction.editReply({ 
                content: `No one is subscribed to notifications for **${username}**.`, 
                flags: 64
            });
        }

        const subscribers = this.pingSubscribers.get(normalizedUsername);
        
        if (!subscribers.has(userId)) {
            return await interaction.editReply({ 
                content: `You're not subscribed to notifications for **${username}**.`, 
                flags: 64
            });
        }

        subscribers.delete(userId);
        
        // Clean up empty subscriber sets
        if (subscribers.size === 0) {
            this.pingSubscribers.delete(normalizedUsername);
        }
        
        await this.saveBotData();

        const actualUsername = this.getActualUsername(normalizedUsername);

        const embed = new EmbedBuilder()
            .setColor(0xff6b6b)
            .setTitle('Notification Removed')
            .setDescription(`You will no longer be pinged when **${actualUsername}** wins a game.`)
            .setTimestamp();

        await interaction.editReply({ embeds: [embed] });
        console.log(`${interaction.user.tag} unsubscribed from notifications for ${actualUsername}`);
    }

    async initializeLastSeenEmbed() {
        try {
            const lastSeenChannel = this.client.channels.cache.find(channel => channel.name === 'last-seen');
            if (!lastSeenChannel) return;

            // Clear existing messages and create new embed
            const messages = await lastSeenChannel.messages.fetch({ limit: 10 });
            await lastSeenChannel.bulkDelete(messages);

            await this.updateLastSeenEmbed();
        } catch (error) {
            console.error('Error initializing last seen embed:', error);
        }
    }

    // Start the last seen updater with faster interval
    startLastSeenUpdater() {
        if (this.lastSeenUpdateInterval) {
            clearInterval(this.lastSeenUpdateInterval);
        }
        
        this.lastSeenUpdateInterval = setInterval(async () => {
            await this.lastSeenEmbed();
        }, this.lastSeenUpdateFrequency);

        console.log(`Started last seen updater (every ${this.lastSeenUpdateFrequency / 1000} seconds)`);
    }

    async updateLastSeenEmbed() {
    try {
        const lastSeenChannel = this.client.channels.cache.find(channel => channel.name === 'last-seen');
        if (!lastSeenChannel) return;

        const totalUsers = this.trackedUsers.size + this.tempUsers.size;
        if (totalUsers === 0) return;

        const currentTime = Date.now();
        let tableText = '| Username | Last Seen |\n|----------|----------|\n';
        
        // Combine all users and sort by last seen
        const allUsers = [];
        
        // Add permanent users
        for (const userData of this.trackedUsers.values()) {
            const normalizedUsername = userData.username.toLowerCase();
            const lastSeenTimestamp = this.lastSeen.get(normalizedUsername) || currentTime;
            allUsers.push({ 
                username: userData.username, 
                lastSeenTimestamp: lastSeenTimestamp 
            });
        }
        
        // Add temporary users
        for (const userData of this.tempUsers.values()) {
            const normalizedUsername = userData.username.toLowerCase();
            const lastSeenTimestamp = this.lastSeen.get(normalizedUsername) || currentTime;
            allUsers.push({ 
                username: userData.username, 
                lastSeenTimestamp: lastSeenTimestamp 
            });
        }

        // Sort by most recent first (highest timestamp = most recent)
        allUsers.sort((a, b) => b.lastSeenTimestamp - a.lastSeenTimestamp);

        // Build the table
        for (const user of allUsers) {
            // Calculate time difference in milliseconds
            const timeDiffMs = currentTime - user.lastSeenTimestamp;
            const timeAgo = this.getTimeAgo(timeDiffMs);
            tableText += `| ${user.username} | ${timeAgo} |\n`;
        }

        const embed = new EmbedBuilder()
            .setColor(0x4a9eff)
            .setTitle('Last Seen Overview')
            .setDescription('```\n' + tableText + '```')
            .setTimestamp()
            .setFooter({ text: 'Updates every minute - Shows time since last game' });

        if (this.lastSeenMessageId) {
            try {
                const message = await lastSeenChannel.messages.fetch(this.lastSeenMessageId);
                await message.edit({ embeds: [embed] });
            } catch (error) {
                // Message doesn't exist, create new one
                console.log('Last seen message not found, creating new one');
                const newMessage = await lastSeenChannel.send({ embeds: [embed] });
                this.lastSeenMessageId = newMessage.id;
                await this.saveBotData();
            }
        } else {
            const newMessage = await lastSeenChannel.send({ embeds: [embed] });
            this.lastSeenMessageId = newMessage.id;
            await this.saveBotData();
        }
    } catch (error) {
        console.error('Error updating last seen embed:', error);
    }
 }

    getTimeAgo(timeDiff) {
        // timeDiff ist bereits in Millisekunden
        const seconds = Math.floor(timeDiff / 1000);
        const minutes = Math.floor(seconds / 60);
        const hours = Math.floor(minutes / 60);
        const days = Math.floor(hours / 24);

        if (days > 0) return `${days}d ago`;
        if (hours > 0) return `${hours}h ago`;
        if (minutes > 0) return `${minutes}m ago`;
        if (seconds > 0) return `${seconds}s ago`;
        return 'just now';
    }

    async checkHotPlayer(username, gamemode, winstreak) {
    const normalizedUsername = username.toLowerCase();
    
    console.log(`[DEBUG] checkHotPlayer called: ${username}, ${gamemode}, streak: ${winstreak}`);
    
    // Initialize hot player alerts for this user if not exists
    if (!this.hotPlayerAlerts.has(normalizedUsername)) {
        this.hotPlayerAlerts.set(normalizedUsername, {});
        console.log(`[DEBUG] Initialized empty alerts for ${username}`);
    }
    
    let userAlerts = this.hotPlayerAlerts.get(normalizedUsername);

    // WICHTIG: Sicherheitscheck falls es eine Zahl statt Objekt ist
    if (typeof userAlerts !== 'object' || userAlerts === null) {
        console.warn(`[WARNING] hotPlayerAlerts for ${username} was type ${typeof userAlerts}, resetting to {}`);
        userAlerts = {};
        this.hotPlayerAlerts.set(normalizedUsername, userAlerts);
    }
    
    // Initialize gamemode-specific alerts if not exists
    if (!userAlerts[gamemode]) {
        userAlerts[gamemode] = 0;
        console.log(`[DEBUG] Initialized ${gamemode} alerts for ${username} to 0`);
    }
    
    const lastMilestone = userAlerts[gamemode];
    console.log(`[DEBUG] Current milestone for ${username} in ${gamemode}: ${lastMilestone}`);
    
    // If winstreak has decreased from the last milestone, reset the milestone tracker
    if (winstreak < lastMilestone) {
        console.log(`[DEBUG] Winstreak decreased: ${winstreak} < ${lastMilestone}, resetting milestone to 0`);
        userAlerts[gamemode] = 0;
        await this.saveBotData();
    }
    
    // Check for 50-win milestones
    if (winstreak > 0 && winstreak % 50 === 0) {
        console.log(`[DEBUG] Checking milestone: winstreak=${winstreak}, lastMilestone=${userAlerts[gamemode]}`);
        
        // Only alert if this is a new milestone (higher than last recorded)
        if (winstreak > userAlerts[gamemode]) {
            console.log(`[DEBUG] New milestone reached! ${winstreak} > ${userAlerts[gamemode]}`);
            userAlerts[gamemode] = winstreak;
            await this.saveBotData();
            
            try {
                const alertChannel = this.client.channels.cache.find(channel => channel.name === 'winstreak-alert');
                if (alertChannel) {
                    const gamemodeDisplay = this.gamemodeNames[gamemode] || gamemode.toUpperCase();
                    
                    const embed = new EmbedBuilder()
                        .setColor(0xff4500)
                        .setTitle('Winstreak Alert!')
                        .addFields(
                            { name: '🎮 Player', value: `**${username}**`, inline: true },
                            { name: '📌 Gamemode', value: gamemodeDisplay, inline: true },
                            { name: '🔥 Streak', value: `**${winstreak} Wins** in a row!`, inline: true }
                        )
                        .setTimestamp();

                    await alertChannel.send({ embeds: [embed] });
                    console.log(`[SUCCESS] Hot player alert sent: ${username} reached ${winstreak} winstreak in ${gamemode}`);
                } else {
                    console.log(`[WARNING] winstreak-alert channel not found`);
                }
            } catch (error) {
                console.error('Error sending hot player alert:', error);
            }
        } else {
            console.log(`[DEBUG] Milestone not new enough: ${winstreak} <= ${userAlerts[gamemode]}`);
        }
    } else {
        console.log(`[DEBUG] Not a milestone: winstreak=${winstreak}, divisible by 50: ${winstreak % 50 === 0}`);
    }
 }

    // Gamemode-specific queue prediction calculation
    calculateQueuePrediction(username, gamemode) {
        const normalizedUsername = username.toLowerCase();
        const userMatchTimes = this.matchTimes.get(normalizedUsername) || {};
        const gamemodeMatchTimes = userMatchTimes[gamemode] || [];
        
        if (gamemodeMatchTimes.length < 2) {
            return null; // Need at least 2 matches for prediction
        }

        // Filter out matches older than 30 minutes and calculate intervals
        const thirtyMinutesAgo = Date.now() - (30 * 60 * 1000);
        const recentMatchTimes = gamemodeMatchTimes.filter(time => time > thirtyMinutesAgo);
        
        if (recentMatchTimes.length < 2) {
            return null; // Need at least 2 recent matches
        }

        // Calculate average time between matches for this specific gamemode
        const intervals = [];
        for (let i = 1; i < recentMatchTimes.length; i++) {
            intervals.push(recentMatchTimes[i] - recentMatchTimes[i - 1]);
        }
        
        // Use weighted average (more recent intervals have more weight)
        let weightedSum = 0;
        let totalWeight = 0;
        for (let i = 0; i < intervals.length; i++) {
            const weight = i + 1; // More recent = higher weight
            weightedSum += intervals[i] * weight;
            totalWeight += weight;
        }
        
        const averageInterval = weightedSum / totalWeight;
        const lastMatchTime = recentMatchTimes[recentMatchTimes.length - 1];
        const predictedNextMatch = lastMatchTime + averageInterval;
        
        const prediction = Math.max(0, Math.floor((predictedNextMatch - Date.now()) / 1000));
        
        // Only return prediction if it's reasonable (between 30 seconds and 20 minutes)
        if (prediction >= 30 && prediction <= 1200) {
            return prediction;
        }
        
        return null;
    }

    async startQueueCountdown(messageId, initialSeconds) {
        // Clear any existing countdown for this message
        if (this.queuePredictionMessages.has(messageId)) {
            clearInterval(this.queuePredictionMessages.get(messageId));
            this.queuePredictionMessages.delete(messageId);
        }

        // Add a max lifetime to prevent infinite intervals
        const maxLifetime = setTimeout(() => {
            if (this.queuePredictionMessages.has(messageId)) {
                clearInterval(this.queuePredictionMessages.get(messageId));
                this.queuePredictionMessages.delete(messageId);
            }
        }, initialSeconds * 1000 + 60000); // Initial time + 1 minute buffer

        let remainingSeconds = initialSeconds;
        console.log(`Starting countdown for message ${messageId} with ${initialSeconds}s`);
        
        const interval = setInterval(async () => {
            remainingSeconds -= 14; // Decrease by 10 seconds each interval
            
            if (remainingSeconds <= 0) {
                clearInterval(interval);
                this.queuePredictionMessages.delete(messageId);
                
                // Optional: Update message to show "Queue time expired" or similar
                try {
                    const channel = await this.client.channels.fetch(process.env.CHANNEL_ID);
                    if (channel) {
                        const message = await channel.messages.fetch(messageId);
                        const embed = message.embeds[0];
                        
                        if (embed) {
                            const newEmbed = EmbedBuilder.from(embed);
                            const fields = [...(newEmbed.data.fields || [])];
                            const queueFieldIndex = fields.findIndex(f => f.name === 'Next Match Prediction');
                            
                            if (queueFieldIndex !== -1) {
                                const currentValue = fields[queueFieldIndex].value;
                                const gamemodeMatch = currentValue.match(/\(([^)]+)\)$/);
                                const gamemodeText = gamemodeMatch ? ` (${gamemodeMatch[1]})` : '';
                                
                                fields[queueFieldIndex] = {
                                    ...fields[queueFieldIndex],
                                    value: `Expired${gamemodeText}`
                                };
                                
                                newEmbed.setFields(fields);
                                await message.edit({ embeds: [newEmbed] });
                                console.log(`Countdown expired for message ${messageId}`);
                            }
                        }
                    }
                } catch (error) {
                    console.error('Error updating expired countdown:', error);
                }
                return;
            }

            try {
                const channel = await this.client.channels.fetch(process.env.CHANNEL_ID);
                if (channel) {
                    const message = await channel.messages.fetch(messageId);
                    const embed = message.embeds[0];
                    
                    if (embed) {
                        const newEmbed = EmbedBuilder.from(embed);
                        
                        // Get current fields and find the queue prediction field
                        const fields = [...(newEmbed.data.fields || [])];
                        const queueFieldIndex = fields.findIndex(f => f.name === 'Next Match Prediction');
                        
                        if (queueFieldIndex !== -1) {
                            const currentValue = fields[queueFieldIndex].value;
                            const gamemodeMatch = currentValue.match(/\(([^)]+)\)$/);
                            const gamemodeText = gamemodeMatch ? ` (${gamemodeMatch[1]})` : '';

                            fields[queueFieldIndex] = {
                                ...fields[queueFieldIndex],
                                value: `~${remainingSeconds}s${gamemodeText}`
                            };

                            newEmbed.setFields(fields);
                            await message.edit({ embeds: [newEmbed] });
                            console.log(`Updated countdown for ${messageId}: ${remainingSeconds}s remaining`);
                        }
                    }
                }
            } catch (error) {
                console.error(`Error updating countdown for message ${messageId}:`, error);
                // If we can't update the message, stop the countdown
                clearInterval(interval);
                this.queuePredictionMessages.delete(messageId);
            }
        }, 14000); // Update every 14 seconds 

        this.queuePredictionMessages.set(messageId, interval);
    }

    parseDuration(duration) {
        const durationMap = {
            '1d': 24 * 60 * 60 * 1000,     // 1 day
            '3d': 3 * 24 * 60 * 60 * 1000, // 3 days  
            '7d': 7 * 24 * 60 * 60 * 1000  // 7 days
        };
        return durationMap[duration] || durationMap['1d'];
    }

    formatDuration(duration) {
        const durationMap = {
            '1d': '1 day',
            '3d': '3 days',
            '7d': '7 days'
        };
        return durationMap[duration] || '1 day';
    }

    getActualUsername(normalizedUsername) {
        if (this.trackedUsers.has(normalizedUsername)) {
            return this.trackedUsers.get(normalizedUsername).username;
        }
        if (this.tempUsers.has(normalizedUsername)) {
            return this.tempUsers.get(normalizedUsername).username;
        }
        return normalizedUsername;
    }

    async fetchPlayerStats(username) {
        try {
            const response = await axios.get(`https://api.playhive.com/v0/game/all/all/${encodeURIComponent(username)}`, {
                timeout: 10000,
                headers: {
                    'User-Agent': 'PersonalTracker'
                }
            });

            // Handle rate limit headers
            const rateLimitRemaining = parseInt(response.headers['x-ratelimit-remaining']) || 120;
            const rateLimitLimit = parseInt(response.headers['x-ratelimit-limit']) || 120;
            
            if (rateLimitRemaining < 10) {
                console.warn(`Rate limit warning: ${rateLimitRemaining}/${rateLimitLimit} requests remaining`);
            }

            return response.data;
        } catch (error) {
            if (error.response?.status === 404) {
                 console.log(`Player ${username} not found (404)`);
                return null;
            }
            if (error.response?.status === 429) {
                const retryAfter = parseInt(error.response.headers['retry-after']) || 60;
                console.warn(`Rate limited. Retry after: ${retryAfter}s`);
                throw new error(`Error fetching stats for ${username}:`, error.message);
            }
            console.error(`Error fetching stats for ${username}:`, error.message);
            throw error;
        }
    }

    extractGameStats(playerData) {
        const gameStats = {};
        
        for (const [gamemode, data] of Object.entries(playerData)) {
            if (typeof data === 'object' && data !== null && !Array.isArray(data)) {
                if (this.isGamemodeData(data)) {
                    gameStats[gamemode] = {
                        played: data.played || 0,
                        victories: data.victories || 0,
                        deaths: data.deaths || 0,
                        kills: data.kills || 0,
                        final_kills: data.final_kills || 0,
                        beds_destroyed: data.beds_destroyed || 0,
                        coins: data.coins || 0,
                        murders: data.murders || 0,
                        murderer_eliminations: data.murderer_eliminations || 0,
                        goals: data.goals || 0,
                    };
                }
            }
        }

        return gameStats;
    }

    isGamemodeData(data) {
        return data.hasOwnProperty('played') || data.hasOwnProperty('victories');
    }

    getTotalGames(gameStats) {
        return Object.values(gameStats).reduce((total, stats) => total + (stats.played || 0), 0);
    }

    getTotalVictories(gameStats) {
        return Object.values(gameStats).reduce((total, stats) => total + (stats.victories || 0), 0);
    }

    async startPolling() {
        if (this.isPolling) return;
        this.isPolling = true;
        console.log(`Started polling every ${this.pollInterval / 1000} seconds`);

        const poll = async () => {
            try {
                await this.checkAllUsers();
            } catch (error) {
                console.error("Polling error:", error);
            }

            setTimeout(poll, this.pollInterval);
        };

        poll();
    }

    async startTempUserCleanup() {
        console.log(`Started temp user cleanup every ${this.tempUserCheckInterval / 1000 / 60} minutes`);

        const cleanup = async () => {
            try {
                await this.cleanupExpiredTempUsers();
            } catch (error) {
                console.error('Temp user cleanup error:', error);
            }
            
            setTimeout(cleanup, this.tempUserCheckInterval);
        };

        setTimeout(cleanup, this.tempUserCheckInterval);
    }

    async cleanupExpiredTempUsers() {
        const now = new Date();
        let removedCount = 0;

        for (const [normalizedUsername, userData] of this.tempUsers.entries()) {
            const expiresAt = new Date(userData.expiresAt);
            
            if (now >= expiresAt) {
                // Remove expired temp user
                this.tempUsers.delete(normalizedUsername);
                this.winstreaks.delete(normalizedUsername);
                this.pingSubscribers.delete(normalizedUsername);
                this.lastSeen.delete(normalizedUsername);
                this.lastWin.delete(normalizedUsername);
                this.matchTimes.delete(normalizedUsername);
                this.hotPlayerAlerts.delete(normalizedUsername);
                this.inactiveUserLastCheck.delete(normalizedUsername);

                for (const [userId, subscribedUsernames] of this.dmSubscribers.entries()) {
                if (subscribedUsernames.has(normalizedUsername)) {
                 subscribedUsernames.delete(normalizedUsername);
                if (subscribedUsernames.size === 0) {
                 this.dmSubscribers.delete(userId);
             }
          }
        }
                removedCount++;
                console.log(`Removed expired temp user: ${userData.username}`);
                
                // Send notification to channel about expiry
                try {
                    const channel = await this.client.channels.fetch(process.env.CHANNEL_ID);
                    if (channel) {
                        const embed = new EmbedBuilder()
                            .setColor(0x888888)
                            .setTitle('Temporary Tracking Expired')
                            .setDescription(`Stopped tracking **${userData.username}** (temporary tracking expired)`)
                            .setTimestamp();
                        
                        await channel.send({ embeds: [embed] });
                    }
                } catch (error) {
                    console.error('Error sending expiry notification:', error);
                }
            }
        }

        if (removedCount > 0) {
            await this.saveBotData();
            await this.updateLastSeenEmbed();
            console.log(`Cleanup complete: removed ${removedCount} expired temp users`);
        }
    }

    async checkAllUsers() {
        const totalUsers = this.trackedUsers.size + this.tempUsers.size;
        if (totalUsers === 0) return;

        console.log(`🔎 Checking ${totalUsers} tracked users...`);

        // Collect all users to check
        const usersToCheck = [
            ...[...this.trackedUsers.entries()].map(([normalizedUsername, userData]) => ({ normalizedUsername, userData })),
            ...[...this.tempUsers.entries()].map(([normalizedUsername, userData]) => ({ normalizedUsername, userData }))
        ];

        const now = Date.now();
        const thirtyMinutesAgo = now - (30 * 60 * 1000); // 30 minutes
        const twoMinutes = 2 * 60 * 1000; // 2 minutes
        
        // Separate users into active and inactive based on last WIN time
        const activeUsers = [];
        const inactiveUsers = [];
        
        for (const { normalizedUsername, userData } of usersToCheck) {
            const lastSeenTime = this.lastSeen.get(normalizedUsername) || now;
            const isInactive = lastSeenTime < thirtyMinutesAgo;
            
            if (isInactive) {
                inactiveUsers.push({ normalizedUsername, userData });
            } else {
                activeUsers.push({ normalizedUsername, userData });
            }
        }
        
        console.log(`👀 Active users: ${activeUsers.length}, Inactive users: ${inactiveUsers.length}`);
        
        // Check active users with normal polling interval
        for (const { normalizedUsername, userData } of activeUsers) {
            try {
                await this.limiter.schedule(async () => {
                    try {
                        await this.checkUserStats(normalizedUsername, userData);
                    } catch (err) {
                        console.error(`Error checking active user ${userData.username}:`, err);
                    }
                });

                // Small delay between active user checks
                await new Promise(res => setTimeout(res, Math.min(1000, this.pollInterval / Math.max(activeUsers.length, 1))));
            } catch (error) {
                console.error(`Error checking active user ${userData.username}:`, error.message);
            }
        }

        // For inactive users, use a separate tracking mechanism
        if (!this.inactiveUserLastCheck) {
            this.inactiveUserLastCheck = new Map();
        }
        
        // Check inactive users every 2 minutes
        for (const { normalizedUsername, userData } of inactiveUsers) {
            try {
                const lastCheck = this.inactiveUserLastCheck.get(normalizedUsername) || 0;
                const timeSinceLastCheck = now - lastCheck;
            
                // Only check inactive users if 2 minutes have passed since last check
                if (timeSinceLastCheck >= twoMinutes) {
                    console.log(`Checking inactive user ${userData.username} (last win: ${new Date(this.lastWin.get(normalizedUsername) || now).toLocaleTimeString()})`);
                
                    await this.limiter.schedule(async () => {
                        try {
                            await this.checkUserStats(normalizedUsername, userData);
                        } catch (err) {
                            console.error(`Error checking inactive user ${userData.username}:`, err);
                        }
                    });
                
                    // Update the inactive user check time (separate from lastSeen)
                    this.inactiveUserLastCheck.set(normalizedUsername, now);
                
                    // Small delay between inactive user checks
                    await new Promise(res => setTimeout(res, 500));
                } else {
                    const remainingTime = Math.floor((twoMinutes - timeSinceLastCheck) / 1000);
                    console.log(`Skipping inactive user ${userData.username} (will check in ${remainingTime}s)`);
                }
            } catch (error) {
                console.error(`Error checking inactive user ${userData.username}:`, error.message);
            }
        }
    }

    async checkUserStats(normalizedUsername, userData) {
        const newPlayerData = await this.fetchPlayerStats(userData.username);
        
        if (!newPlayerData) {
            console.warn(`Could not fetch data for ${userData.username}`);
            return;
        }

        const newGameStats = this.extractGameStats(newPlayerData);
        const oldGameStats = userData.lastStats || {};

        // Initialize winstreaks for this user if not exists
        if (!this.winstreaks.has(normalizedUsername)) {
            this.winstreaks.set(normalizedUsername, {});
        }
        const userWinstreaks = this.winstreaks.get(normalizedUsername);

        // Initialize match times if not exists (gamemode-specific)
        if (!this.matchTimes.has(normalizedUsername)) {
            const gamemodeMatchTimes = {};
            for (const gamemode of Object.keys(newGameStats)) {
                gamemodeMatchTimes[gamemode] = [];
            }
            this.matchTimes.set(normalizedUsername, gamemodeMatchTimes);
        }

        let hasNewMatch = false;
        let hasNewWin = false;
        const currentTime = Date.now();

        // Check each gamemode for changes
        for (const [gamemode, newStats] of Object.entries(newGameStats)) {
            // Initialize winstreak for this gamemode if not exists
            if (!userWinstreaks[gamemode]) {
                userWinstreaks[gamemode] = 0;
            }

            // Initialize match times for this gamemode if not exists
            const userMatchTimes = this.matchTimes.get(normalizedUsername);
            if (!userMatchTimes[gamemode]) {
                userMatchTimes[gamemode] = [];
            }

            // Fix: Ensure oldStats is always an object with default values
            const oldStats = (oldGameStats[gamemode] && typeof oldGameStats[gamemode] === 'object') 
                ? oldGameStats[gamemode] 
                : { played: 0, victories: 0, deaths: 0, kills: 0, final_kills: 0, beds_destroyed: 0, coins: 0, murders: 0, murderer_eliminations: 0, goals: 0 };
            
            // Ensure newStats has all required properties
            const safeNewStats = {
                played: newStats.played || 0,
                victories: newStats.victories || 0,
                deaths: newStats.deaths || 0,
                kills: newStats.kills || 0,
                final_kills: newStats.final_kills || 0,
                beds_destroyed: newStats.beds_destroyed || 0,
                coins: newStats.coins || 0,
                murders: newStats.murders || 0,
                murderer_eliminations: newStats.murderer_eliminations || 0,
                goals: newStats.goals || 0,
            };
            
            const gamesPlayedDiff = safeNewStats.played - oldStats.played;
            
            if (gamesPlayedDiff > 0) {
                hasNewMatch = true;
                const victoriesDiff = safeNewStats.victories - oldStats.victories;
                
                console.log(`${userData.username} played ${gamesPlayedDiff} ${gamemode} game(s), won ${victoriesDiff}`);
                
                // Add match time for this gamemode
                userMatchTimes[gamemode].push(currentTime);
                
                // Keep only last 10 match times per gamemode for prediction calculation
                if (userMatchTimes[gamemode].length > 10) {
                    userMatchTimes[gamemode].shift();
                }
                
                // Check if there were any wins
                if (victoriesDiff > 0) {
                    hasNewWin = true;
                }
                
                // Process each game result
                for (let i = 0; i < gamesPlayedDiff; i++) {
                    const isWin = i < victoriesDiff;
                    
                    // Update winstreak for THIS specific gamemode only
                    if (isWin) {
                        userWinstreaks[gamemode] = (userWinstreaks[gamemode] || 0) + 1;
                    } else {
                        userWinstreaks[gamemode] = 0;
                    }
                    
                    // Calculate stats for this specific game (approximation)
                    const gameSpecificStats = {
                        kills: Math.max(0, Math.floor((safeNewStats.kills - oldStats.kills) / gamesPlayedDiff)),
                        final_kills: Math.max(0, Math.floor((safeNewStats.final_kills - oldStats.final_kills) / gamesPlayedDiff)),
                        beds_destroyed: Math.max(0, Math.floor((safeNewStats.beds_destroyed - oldStats.beds_destroyed) / gamesPlayedDiff)),
                        deaths: Math.max(0, Math.floor((safeNewStats.deaths - oldStats.deaths) / gamesPlayedDiff)),
                        coins: Math.max(0, Math.floor((safeNewStats.coins - oldStats.coins) / gamesPlayedDiff)),
                        murders: Math.max(0, Math.floor((safeNewStats.murders - oldStats.murders) / gamesPlayedDiff)),
                        murderer_eliminations: Math.max(0, Math.floor((safeNewStats.murderer_eliminations - oldStats.murderer_eliminations) / gamesPlayedDiff)),
                        goals: Math.max(0, Math.floor((safeNewStats.goals - oldStats.goals) / gamesPlayedDiff)),
                    };
                    
                    await this.sendGameNotification(userData.username, gamemode, isWin, gameSpecificStats);
                    
                    // Check for hot player milestones
                    if (isWin) {
                        await this.checkHotPlayer(userData.username, gamemode, userWinstreaks[gamemode]);
                    }
                }
            }
        }

        // Update last seen ONLY if there was a new match (resets to current time)
        if (hasNewMatch) {
            this.lastSeen.set(normalizedUsername, Date.now());
            console.log(`Updated last seen for ${userData.username} due to new match`);
            
            // Update last seen embed immediately when a match is detected
            await this.updateLastSeenEmbed();
        }

        // Update last win time ONLY if there was actually a win (this controls activity status)
        if (hasNewWin) {
            this.lastWin.set(normalizedUsername, currentTime);
            console.log(`Updated last win for ${userData.username} - user is now ACTIVE`);
        }

        // Update stored stats with the safe new stats
        const safeGameStats = {};
        for (const [gamemode, stats] of Object.entries(newGameStats)) {
            safeGameStats[gamemode] = {
                played: stats.played || 0,
                victories: stats.victories || 0,
                deaths: stats.deaths || 0,
                kills: stats.kills || 0,
                final_kills: stats.final_kills || 0,
                beds_destroyed: stats.beds_destroyed || 0,
                coins: stats.coins || 0,
                murders: stats.murders || 0,
                murderer_eliminations: stats.murderer_eliminations || 0,
                goals: stats.goals || 0
            };
        }
        
        userData.lastStats = safeGameStats;
        
        // Update the appropriate map
        if (this.trackedUsers.has(normalizedUsername)) {
            this.trackedUsers.set(normalizedUsername, userData);
        } else if (this.tempUsers.has(normalizedUsername)) {
            this.tempUsers.set(normalizedUsername, userData);
        }
        
        await this.saveBotData();
    }

    async sendGameNotification(username, gamemode, isWin, gameStats) {
        const channel = await this.client.channels.fetch(process.env.CHANNEL_ID);
        
        if (!channel) {
            console.error('Could not find notification channel');
            return;
        }

        const normalizedUsername = username.toLowerCase();
        const gamemodeDisplay = this.gamemodeNames[gamemode] || gamemode.toUpperCase();
        const result = isWin ? 'WIN' : 'LOSS';
        const color = isWin ? 0x00ff00 : 0xff0000;

        //Gamemode Detection - ADD GAME TO HISTORY
        this.gamemodeDetector.addGameToHistory(username, gameStats);


        //Detect Gamemode for BED only
        let gamemodePercentages = null;
        let gamemodePercentageText = '';

        if (gamemode === 'bed') {
            gamemodePercentages = this.gamemodeDetector.detectGamemodeWithHistory(username, gameStats);
            gamemodePercentageText = this.gamemodeDetector.formatPercentages(gamemodePercentages);
        }

        //Update Daily K/D only for Gamemodes with Kills and Deaths
        const hasKillsDeaths = gameStats.kills > 0 || gameStats.deaths > 0;
        if (hasKillsDeaths) {
            this.updateDailyStats(username, gamemode, gameStats.kills, gameStats.deaths);
        }
        
        // Get the winstreak for THIS specific gamemode
        const userWinstreaks = this.winstreaks.get(normalizedUsername) || {};
        const gamemodeWinstreak = userWinstreaks[gamemode] || 0;

        const embedFields = [
            { name: '🎮 Player', value: `**${username}**`, inline: true },
            { name: '📌 Gamemode', value: `**${gamemodeDisplay}**`, inline: true },
            { name: '🏁 Result', value: result, inline: true },
            { name: '🔥 Winstreak', value: `${gamemodeWinstreak} (${gamemodeDisplay})`, inline: true },
            { name: '⚔️ Kills', value: gameStats.kills.toString(), inline: true },
            { name: '💀 Deaths', value: gameStats.deaths.toString(), inline: true }
        ];

        //Add Daily K/D if vilable 
        if (hasKillsDeaths) {
            const dailyKD = this.getDailyKD(username, gamemode);
            if (dailyKD !== null) {
                const userStats = this.dailyStats.get(normalizedUsername);
                if (userStats && userStats[gamemode]) {
                    const dailyStats = userStats[gamemode]
                embedFields.push({
                    name: 'Daily K/D',
                    value: `${dailyKD} (${dailyStats.kills}K/${dailyStats.deaths}D)`,
                    inline: true
                });
            }
        }

        // Add gamemode-specific stats
        if (gameStats.final_kills > 0) {
            embedFields.push({ name: '🎯 Final Kills', value: gameStats.final_kills.toString(), inline: true });
        }
        
        if (gameStats.beds_destroyed > 0) {
            embedFields.push({ name: '🛏️ Beds Destroyed', value: gameStats.beds_destroyed.toString(), inline: true });
        }

        if (gameStats.coins > 0) {
            embedFields.push({ name: '🪙 Coins Collected', value: gameStats.coins.toString(), inline: true });
        }

        if (gameStats.murders > 0) {
            embedFields.push({ name: '🗡️ Murder Kills', value: gameStats.murders.toString(), inline: true });
        }

        if (gameStats.murderer_eliminations > 0) {
            embedFields.push({ name: '🏹 Murder Eliminations', value: gameStats.murderer_eliminations.toString(), inline: true });
        }

        if (gameStats.goals > 0) {
            embedFields.push({ name: '🥅 Goals', value: gameStats.goals.toString(), inline: true });
        }

        // Add gamemode-specific queue prediction
        const queuePrediction = this.calculateQueuePrediction(username, gamemode);
        if (queuePrediction !== null && queuePrediction > 0) {
            embedFields.push({ 
                name: 'Next Match Prediction', 
                value: `~${queuePrediction}s (${gamemodeDisplay})`, 
                inline: true 
            });
        }

        const embed = new EmbedBuilder()
            .setColor(color)
            .setTitle(`${result} Game Result`)
            .setFields(embedFields)
            .setTimestamp()
            .setFooter({ text: 'Hive Bot' });

            //Add GameMode Detection as footer if possible
            if (gamemodePercentageText) {
                embed.setFooter({ text: `${gamemodePercentageText}` });
            }

        let messageContent = '';
        
        // Add ping mentions for wins
        if (isWin && this.pingSubscribers.has(normalizedUsername)) {
            const subscribers = this.pingSubscribers.get(normalizedUsername);
            if (subscribers.size > 0) {
                const mentions = Array.from(subscribers).map(userId => `<@${userId}>`).join(' ');
                messageContent = `${username} won a ${gamemodeDisplay} game! ${mentions}`;
            }
        }

        try {
            const messageOptions = { embeds: [embed] };
            if (messageContent) {
                messageOptions.content = messageContent;
            }
            
            const sentMessage = await channel.send(messageOptions);
                     
            // Start queue prediction countdown if applicable (only for wins to avoid spam)
            if (queuePrediction !== null && queuePrediction > 0 && isWin) {
                console.log(`Starting countdown for ${username}: ${queuePrediction}s`);
                await this.startQueueCountdown(sentMessage.id, queuePrediction);
            }
            
            console.log(`Sent notification: ${username} ${result} in ${gamemodeDisplay} (${gamemodeDisplay} winstreak: ${gamemodeWinstreak})`);

            for (const [userId, subscribedUsernames] of this.dmSubscribers.entries()) {
    if (subscribedUsernames.has(normalizedUsername)) {
        try {
            const user = await this.client.users.fetch(userId);
            if (user) {
                const dmEmbed = new EmbedBuilder()
                    .setColor(color)
                    .setTitle(`${result} - ${username}`)
                    .setDescription(`**${username}** ${result === 'WIN' ? 'won' : 'lost'} a ${gamemodeDisplay} game!`)
                    .addFields(
                        { name: 'Winstreak', value: `${gamemodeWinstreak} (${gamemodeDisplay})`, inline: true },
                        { name: 'Kills', value: gameStats.kills.toString(), inline: true },
                        { name: 'Deaths', value: gameStats.deaths.toString(), inline: true }
                    )
                    .setTimestamp()

                //Add Daily K/D to DM
                if (hasKillsDeaths) {
                    const dailyKD = this.getDailyKD(username,gamemode);
                if (dailyKD !== null) {
                    const userStats = this.dailyStats.get(normalizedUsername)
                    if (userStats && userStats[gamemode]);
                    dmEmbed.addFields({
                        name: 'Daily K/D (${gamemodeDisplay})',
                        value: `${dailyKD} (${dailyStats.kills}K/${dailyStats.deaths}D)`, 
                        inline: true
                    });
                  }
                }

                // Add Queue Prediction to DM
                if (queuePrediction !== null && queuePrediction > 0) {
                    dmEmbed.addFields({
                        name: 'Next Match',
                        value: `~${queuePrediction}s`,
                        inline: true
                    });
                }

                // Add gamemode-specific stats to DM
                if (gameStats.final_kills > 0) dmEmbed.addFields({ name: 'Final Kills', value: gameStats.final_kills.toString(), inline: true });
                if (gameStats.beds_destroyed > 0) dmEmbed.addFields({ name: 'Beds Destroyed', value: gameStats.beds_destroyed.toString(), inline: true });
                if (gameStats.coins > 0) dmEmbed.addFields({ name: 'Coins', value: gameStats.coins.toString(), inline: true });
                if (gameStats.murders > 0) dmEmbed.addFields({ name: 'Murders', value: gameStats.murders.toString(), inline: true });
                if (gameStats.murderer_eliminations > 0) dmEmbed.addFields({ name: 'Murder Eliminations', value: gameStats.murderer_eliminations.toString(), inline: true });
                if (gameStats.goals > 0) dmEmbed.addFields({ name: 'Goals', value: gameStats.goals.toString(), inline: true });

                await user.send({ embeds: [dmEmbed] });
                console.log(`Sent DM notification to ${user.tag} for ${username} ${result}`);
            }
        } catch (error) {
            console.error(`Error sending DM to ${userId} for ${username}:`, error);
            // Optionally remove failed DM subscriptions
            if (error.code === 50007) { // Cannot send messages to this user
                subscribedUsernames.delete(normalizedUsername);
                if (subscribedUsernames.size === 0) {
                    this.dmSubscribers.delete(userId);
                }
                await this.saveBotData();
                console.log(`Removed DM subscription for ${userId} (cannot send messages)`);
             }
          }
        }
    }

        } catch (error) {
            console.error('Error sending notification:', error);
        }
    }
  }



    async loadBotData() {
        try {
            const data = await fs.readFile(this.dataFile, 'utf8');
            const parsedData = JSON.parse(data);
            
            // Load tracked users
            this.trackedUsers = new Map(Object.entries(parsedData.trackedUsers || {}));
            
            // Load temporary users
            this.tempUsers = new Map(Object.entries(parsedData.tempUsers || {}));
            
            // Load winstreaks (now supports nested objects for gamemode-specific streaks)
            this.winstreaks = new Map();
            if (parsedData.winstreaks) {
                for (const [username, winstreakData] of Object.entries(parsedData.winstreaks)) {
                    // Handle both old format (number) and new format (object)
                    if (typeof winstreakData === 'number') {
                        // Convert old single winstreak to object format
                        this.winstreaks.set(username, {});
                    } else if (typeof winstreakData === 'object') {
                        this.winstreaks.set(username, winstreakData);
                    }
                }
            }
            
            // Load ping subscribers (convert arrays back to Sets)
            this.pingSubscribers = new Map();
            if (parsedData.pingSubscribers) {
                for (const [username, subscriberArray] of Object.entries(parsedData.pingSubscribers)) {
                    this.pingSubscribers.set(username, new Set(subscriberArray));
                }
            }

            //Load DM subscribers 
            this.dmSubscribers = new Map();
            if (parsedData.dmSubscribers) {
                for (const [userId, usernameArray] of Object.entries(parsedData.dmSubscribers)) {
                    this.dmSubscribers.set(userId, new Set(usernameArray));
                }
            }

            // Load last seen data
            this.lastSeen = new Map(Object.entries(parsedData.lastSeen || {}));
            
            // Load last win data (for activity tracking)
            this.lastWin = new Map(Object.entries(parsedData.lastWin || {}));

            // Load match times (now gamemode-specific)
            this.matchTimes = new Map();
            if (parsedData.matchTimes) {
                for (const [username, matchTimeData] of Object.entries(parsedData.matchTimes)) {
                    // Handle both old format (array) and new format (object)
                    if (Array.isArray(matchTimeData)) {
                        // Convert old single array to object format
                        this.matchTimes.set(username, {});
                    } else if (typeof matchTimeData === 'object') {
                        this.matchTimes.set(username, matchTimeData);
                    }
                }
            }

            // Load hot player alerts
            this.hotPlayerAlerts = new Map();
            if (parsedData.hotPlayerAlerts) {
               for (const [username, alertData] of Object.entries(parsedData.hotPlayerAlerts)) {
                   // Handle both old format (number) and new format (object)
                   if (typeof alertData === 'number') {
                       // Convert old single alert to object format
                       this.hotPlayerAlerts.set(username, {});
                       console.log(`Converted old hotPlayerAlerts format for ${username}: ${alertData} -> {}`);
                    } else if (typeof alertData === 'object' && alertData !== null) {
                       this.hotPlayerAlerts.set(username, alertData);
                    } else {
                        this.hotPlayerAlerts.set(username, {});
                        console.log(`⚠️ Invalid hotPlayerAlerts for ${username}, reset to {}`);
                    }
                }
            }

            //Load only-view users
            this.onlyViewUsers = new Set(parsedData.onlyViewUsers || []); 

            // Load Daily Stats
            this.dailyStats = new Map();
            if (parsedData.dailyStats) {
                for (const [username, stats] of Object.entries(parsedData.dailyStats)) {
                    const normalizedUsername = username.toLowerCase();

                // Check ob altes Format (direkt kills/deaths) oder neues Format (gamemode nested)
                if (stats.hasOwnProperty('kills') && stats.hasOwnProperty('deaths') && stats.hasOwnProperty('lastResetDate')) {
                    // Altes Format - konvertiere zu neuem Format
                    console.log(`Migrating old daily stats format for ${username} (use new gamemode-specific format)`);
                    this.dailyStats.set(normalizedUsername, {});
                } else {
                    // Neues Format
                    const validatedStats = {};
                    for (const [gamemode, gamemodeStats] of Object.entries(stats)) {
                        if (gamemodeStats && 
                            typeof gamemodeStats === 'object' && 
                            typeof gamemodeStats.kills === 'number' && 
                            typeof gamemodeStats.deaths === 'number' &&
                            gamemodeStats.lastResetDate) {
                            validatedStats[gamemode] = gamemodeStats;
                        } else {
                            console.log(`⚠️ Invalid gamemode stats for ${username}/${gamemode}, skipping`);
                        }
                    }
                    this.dailyStats.set(normalizedUsername, validatedStats);
                }
            }
        }

        console.log(`Loaded daily stats for ${this.dailyStats.size} users`);

            // Load last seen message ID
            this.lastSeenMessageId = parsedData.lastSeenMessageId || null;
            
            const totalUsers = this.trackedUsers.size + this.tempUsers.size;
            console.log(`Loaded ${totalUsers} tracked users (${this.trackedUsers.size} permanent, ${this.tempUsers.size} temporary)`);
            console.log(`Loaded ${this.pingSubscribers.size} ping subscriptions`);
        } catch (error) {
            if (error.code === 'ENOENT') {
                console.log('No existing data file found, starting fresh');
                this.trackedUsers = new Map();
                this.tempUsers = new Map();
                this.winstreaks = new Map();
                this.pingSubscribers = new Map();
                this.lastSeen = new Map();
                this.lastWin = new Map();
                this.matchTimes = new Map();
                this.hotPlayerAlerts = new Map();
                this.dailyStats = new Map();
            }    else {
                console.error('Error loading bot data:', error);
                this.trackedUsers = new Map();
                this.tempUsers = new Map();
                this.onlyViewUsers = new Set();
                this.winstreaks = new Map();
                this.dmSubscribers = new Map();
                this.pingSubscribers = new Map();
                this.lastSeen = new Map();
                this.lastWin = new Map();
                this.matchTimes = new Map();
                this.hotPlayerAlerts = new Map();
                this.inactiveUserLastCheck = new Map();
                this.dailyStats = new Map();
            }
        }
    }
  

    async saveBotData() {
    try {
        const dataToSave = {
            trackedUsers: Object.fromEntries(this.trackedUsers || new Map()),
            tempUsers: Object.fromEntries(this.tempUsers || new Map()),
            onlyViewUsers: Array.from(this.onlyViewUsers || new Set()),
            winstreaks: Object.fromEntries(this.winstreaks || new Map()),
            dmSubscribers: Object.fromEntries(
                Array.from((this.dmSubscribers || new Map()).entries()).map(([userId, usernameSet]) => [userId, Array.from(usernameSet)])
            ),
            pingSubscribers: Object.fromEntries(
                Array.from((this.pingSubscribers || new Map()).entries()).map(([username, subscriberSet]) => [username, Array.from(subscriberSet)])
            ),
            lastSeen: Object.fromEntries(this.lastSeen || new Map()),
            lastWin: Object.fromEntries(this.lastWin || new Map()),
            matchTimes: Object.fromEntries(this.matchTimes || new Map()),
            hotPlayerAlerts: Object.fromEntries(this.hotPlayerAlerts || new Map()),
            lastSeenMessageId: this.lastSeenMessageId,
            inactiveUserLastCheck: Object.fromEntries(this.inactiveUserLastCheck || new Map()),
            dailyStats: Object.fromEntries(this.dailyStats || new Map())
        };

        await fs.writeFile(this.dataFile, JSON.stringify(dataToSave, null, 2));
    } catch (error) {
        console.error('❌ Error saving bot data:', error);
    }
  }
}

// Add this new class after the HiveSnipingBot class constructor, before the init() method

class GamemodeDetector {
    constructor() {
        this.gameHistory = new Map();
        this.maxHistorySize = 10;
        
        // Gewichtungskonfiguration für History-basierte Anpassungen
        this.historyWeights = {
            beds_destroyed: {
                // Wenn durchschnittlich > 3 Betten
                high_threshold: 3,
                high_adjustments: {
                    'bed-solos': 1.4,      // +40% für Solos
                    'bed-duos': 1.3,       // +30% für Duos
                    'bed-squads': 0.6,     // -40% für Squads
                    'bed-manor': 0.6,      // -40% für Manor
                    'bed-mega': 0        // -100% für Mega
                },
                // Wenn durchschnittlich 2-3 Betten
                medium_threshold: 2,
                medium_adjustments: {
                    'bed-solos': 1.1,      // +10% für Solos
                    'bed-duos': 1.15,      // +15% für Duos
                    'bed-squads': 1.02,     // +2% für Squads
                    'bed-manor': 1.034,      // +3,4% für Manor
                    'bed-mega': 0       // -100% für Mega
                },
                // Wenn durchschnittlich < 2 Betten
                low_adjustments: {
                    'bed-solos': 0.25,      // -75% für Solos
                    'bed-duos': 0.26,      // -74% für Duos
                    'bed-squads': 1.15,     // +15% für Squads
                    'bed-manor': 1.20,     // +20% für Manor
                    'bed-mega': 1.1        // +10% für Mega
                }
            },
            kills: {
                // Wenn durchschnittlich > 12 Kills
                high_threshold: 12,
                high_adjustments: {
                    'bed-solos': 1.3,      // +30% für Solos
                    'bed-duos': 1.25,      // +25% für Duos
                    'bed-squads': 0.7,     // -30% für Squads
                    'bed-manor': 0.83,     // -17% für Manor
                    'bed-mega': 1.1        // +10% für Mega
                },
                // Wenn durchschnittlich 7-12 Kills
                medium_threshold: 7,
                medium_adjustments: {
                    'bed-solos': 1.1,      // +10% für Solos
                    'bed-duos': 1.1,       // +10% für Duos
                    'bed-squads': 0.95,    // -5% für Squads
                    'bed-manor': 1.1,     // +10% für Manor
                    'bed-mega': 1.0        // Neutral für Mega
                },
                // Wenn durchschnittlich < 7 Kills
                low_adjustments: {
                    'bed-solos': 0.7,      // -30% für Solos
                    'bed-duos': 0.75,      // -25% für Duos
                    'bed-squads': 1.2,     // +20% für Squads
                    'bed-manor': 1.15,     // +15% für Manor
                    'bed-mega': 0.98        // -2% für Mega
                }
            },
            final_kills: {
                // Wenn durchschnittlich > 5 Final Kills
                high_threshold: 5,
                high_adjustments: {
                    'bed-solos': 0.95,      // -5% für Solos
                    'bed-duos': 1,       // Neutral für Duos
                    'bed-squads': 0.9,     // -10% für Squads
                    'bed-manor': 0.88,      // -12% für Manor
                    'bed-mega': 0.98        // -2% für Mega
                },
                // Wenn durchschnittlich 3-5 Final Kills
                medium_threshold: 3,
                medium_adjustments: {
                    'bed-solos': 1.1,      // +10% für Solos
                    'bed-duos': 1,      // Neutral für Duos
                    'bed-squads': 0.98,    // -2% für Squads
                    'bed-manor': 1.15,      // +15% für Manor
                    'bed-mega': 0.98       // -2% für Mega
                },
                // Wenn durchschnittlich < 3 Final Kills
                low_adjustments: {
                    'bed-solos': 0.9,      // -10% für Solos
                    'bed-duos': 0.85,      // -15% für Duos
                    'bed-squads': 1.15,    // +15% für Squads
                    'bed-manor': 1.1,      // +10% für Manor
                    'bed-mega': 1.05       // +5% für Mega
                }
            },
            deaths: {
                // Wenn durchschnittlich > 3 Deaths
                high_threshold: 3,
                high_adjustments: {
                    'bed-solos': 1.2,      // +20% für Solos
                    'bed-duos': 1.2,       // +20% für Duos
                    'bed-squads': 0.2,     // -80% für Squads
                    'bed-manor': 0.7,     // -30% für Manor
                    'bed-mega': 0.10        // -90% für Mega
                },
                // Wenn durchschnittlich 1-3 Deaths
                medium_threshold: 1,
                medium_adjustments: {
                    'bed-solos': 1.05,      // -5% für Solos
                    'bed-duos': 1.085,      // +8,5% für Duos
                    'bed-squads': 1.0,     // Neutral für Squads
                    'bed-manor': 1.0,      // Neutral für Manor
                    'bed-mega': 0.95       // -5% für Mega
                },
                // Wenn durchschnittlich < 1 Deaths (sehr wenig)
                low_adjustments: {
                    'bed-solos': 0.6,      // -40% für Solos
                    'bed-duos': 0.6,       // -40% für Duos
                    'bed-squads': 1.12,     // +12% für Squads
                    'bed-manor': 0.9,      // -10% für Manor
                    'bed-mega': 1.05       // +5% für Mega
                }
            }
        };
        
        this.gamemodeRanges = {
            'bed-solos': {
                final_kills: { max: 7, realistic_min: 3, realistic_max: 4 },
                kills: { max: 20, realistic_min: 7, realistic_max: 13 },
                beds_destroyed: { max: 7, realistic_min: 2, realistic_max: 4 },
                deaths: { max: 7, realistic_min: 0, realistic_max: 4 }
            },
            'bed-duos': {
                final_kills: { max: 14, realistic_min: 3, realistic_max: 6 },
                kills: { max: 25, realistic_min: 9, realistic_max: 15 },
                beds_destroyed: { max: 7, realistic_min: 2, realistic_max: 4 },
                deaths: { max: 7, realistic_min: 0, realistic_max: 4 }
            },
            'bed-squads': {
                final_kills: { max: 12, realistic_min: 0, realistic_max: 5 },
                kills: { max: 14, realistic_min: 0, realistic_max: 6 },
                beds_destroyed: { max: 3, realistic_min: 0, realistic_max: 2 },
                deaths: { max: 5, realistic_min: 0, realistic_max: 4 }
            },
            'bed-manor': {
                final_kills: { max: 15, realistic_min: 1.5, realistic_max: 6 },
                kills: { max: 15, realistic_min: 2, realistic_max: 9 },
                beds_destroyed: { max: 3, realistic_min: 0, realistic_max: 2 },
                deaths: { max: 5, realistic_min: 0, realistic_max: 2 }
            },
            'bed-mega': {
                final_kills: { max: 12, realistic_min: 0, realistic_max: 4 },
                kills: { max: 25, realistic_min: 4, realistic_max: 12 },
                beds_destroyed: { max: 1, realistic_min: 0, realistic_max: 1 },
                deaths: { max: 3, realistic_min: 0, realistic_max: 2 }
            }
        };
    }

    addGameToHistory(username, gameStats) {
        const normalizedUsername = username.toLowerCase();
        if (!this.gameHistory.has(normalizedUsername)) {
            this.gameHistory.set(normalizedUsername, []);
        }
        
        const history = this.gameHistory.get(normalizedUsername);
        history.push({
            ...gameStats,
            timestamp: Date.now()
        });
        
        if (history.length > this.maxHistorySize) {
            history.shift();
        }
    }

    calculateHistoryAverages(username) {
        const normalizedUsername = username.toLowerCase();
        const history = this.gameHistory.get(normalizedUsername) || [];
        
        if (history.length === 0) {
            return null;
        }

        const avgStats = { kills: 0, final_kills: 0, beds_destroyed: 0, deaths: 0 };
        
        for (const game of history) {
            avgStats.kills += game.kills || 0;
            avgStats.final_kills += game.final_kills || 0;
            avgStats.beds_destroyed += game.beds_destroyed || 0;
            avgStats.deaths += game.deaths || 0;
        }

        const gameCount = history.length;
        avgStats.kills /= gameCount;
        avgStats.final_kills /= gameCount;
        avgStats.beds_destroyed /= gameCount;
        avgStats.deaths /= gameCount;

        return avgStats;
    }

    applyHistoryAdjustments(percentages, avgStats) {
        if (!avgStats) return percentages;

        const adjustedPercentages = { ...percentages };

        // Für jede Stat-Kategorie
        for (const [statName, config] of Object.entries(this.historyWeights)) {
            const statValue = avgStats[statName];
            let adjustments;

            // Wähle die richtige Adjustment-Kategorie basierend auf Schwellenwerten
            if (statValue >= config.high_threshold) {
                adjustments = config.high_adjustments;
            } else if (statValue >= config.medium_threshold) {
                adjustments = config.medium_adjustments;
            } else {
                adjustments = config.low_adjustments;
            }

            // Wende Adjustments auf alle Gamemodes an
            for (const [mode, multiplier] of Object.entries(adjustments)) {
                adjustedPercentages[mode] = Math.floor(adjustedPercentages[mode] * multiplier);
            }
        }

        // Normalisiere zurück auf 100%
        const total = Object.values(adjustedPercentages).reduce((sum, val) => sum + Math.max(0, val), 0);
        if (total > 0) {
            for (const mode of Object.keys(adjustedPercentages)) {
                adjustedPercentages[mode] = Math.max(0, Math.round((adjustedPercentages[mode] / total) * 100));
            }
        }

        return adjustedPercentages;
    }

    detectGamemode(gameStats) {
        const { kills = 0, final_kills = 0, beds_destroyed = 0, deaths = 0 } = gameStats;
        
        const scores = {
            'bed-solos': 100,
            'bed-duos': 100,
            'bed-squads': 100,
            'bed-manor': 100,
            'bed-mega': 100
        };

        // Hard exclusions
        if (beds_destroyed > 1) scores['bed-mega'] = 0;
        if (beds_destroyed > 3) {
            scores['bed-squads'] = 0;
            scores['bed-manor'] = 0;
            scores['bed-mega'] = 0;
        }
        if (kills > 15) {
            scores['bed-solos'] = 0;
            scores['bed-duos'] = 0;
        }
        if (final_kills > 7) scores['bed-solos'] = 0;

        // Weighted adjustments
        if (deaths === 0 && beds_destroyed >= 3) {
            scores['bed-solos'] += 20;
            scores['bed-duos'] += 10;
        }

        for (const [mode, ranges] of Object.entries(this.gamemodeRanges)) {
            if (scores[mode] === 0) continue;
            
            const stats = [
                { value: kills, range: ranges.kills },
                { value: final_kills, range: ranges.final_kills },
                { value: beds_destroyed, range: ranges.beds_destroyed },
                { value: deaths, range: ranges.deaths }
            ];
            
            for (const stat of stats) {
                if (stat.value >= stat.range.realistic_min && stat.value <= stat.range.realistic_max) {
                    scores[mode] += 15;
                } else if (stat.value <= stat.range.max) {
                    scores[mode] -= 5;
                } else {
                    scores[mode] -= 30;
                }
            }
        }

        const totalScore = Object.values(scores).reduce((sum, score) => sum + Math.max(0, score), 0);
        const percentages = {};
        
        if (totalScore > 0) {
            for (const [mode, score] of Object.entries(scores)) {
                percentages[mode] = Math.max(0, Math.round((score / totalScore) * 100));
            }
        } else {
            for (const mode of Object.keys(scores)) {
                percentages[mode] = 20;
            }
        }

        return percentages;
    }

    detectGamemodeWithHistory(username, currentGameStats) {
    const normalizedUsername = username.toLowerCase();
    const history = this.gameHistory.get(normalizedUsername) || [];
    
    // Basis-Detection für aktuelles Spiel
    let percentages = this.detectGamemode(currentGameStats);
    
    // Wenn genug History vorhanden, wende Anpassungen an
    if (history.length >= 3) {
        const avgStats = this.calculateHistoryAverages(username);
        percentages = this.applyHistoryAdjustments(percentages, avgStats);
        
        // WICHTIG: Strikte Hard Exclusions basierend auf History
        
        // Mega: Durchschnitt > 1 Bett = UNMÖGLICH
        if (avgStats.beds_destroyed > 1.0) {
            percentages['bed-mega'] = 0;
            console.log(`🚫 Mega excluded: avg beds ${avgStats.beds_destroyed.toFixed(1)} > 1.0`);
        }
        
        // Squads/Manor: Durchschnitt > 3 Betten = UNMÖGLICH
        if (avgStats.beds_destroyed > 3.0) {
            percentages['bed-squads'] = 0;
            percentages['bed-manor'] = 0;
            console.log(`🚫 Squads/Manor excluded: avg beds ${avgStats.beds_destroyed.toFixed(1)} > 3.0`);
        }
        
        // Squads/Manor: Durchschnitt > 2.1 Betten = SEHR UNWAHRSCHEINLICH
        if (avgStats.beds_destroyed > 2.1) {
            percentages['bed-squads'] = Math.floor(percentages['bed-squads'] * 0.3);
            percentages['bed-manor'] = Math.floor(percentages['bed-manor'] * 0.3);
            console.log(`⚠️ Squads/Manor heavily penalized: avg beds ${avgStats.beds_destroyed.toFixed(1)} > 2.1`);
        }
        
        // Solos/Duos: Durchschnitt > 15 Kills = UNMÖGLICH
        if (avgStats.kills > 15) {
            percentages['bed-solos'] = 0;
            percentages['bed-duos'] = 0;
            percentages['bed-squads'] = 0;
            console.log(`🚫 Solos/Duos excluded: avg kills ${avgStats.kills.toFixed(1)} > 15`);
        }
        
        // Solos/Mega: Durchschnitt > 7 Final Kills = UNMÖGLICH
        if (avgStats.final_kills > 7) {
            percentages['bed-solos'] = 0;
            percentages['bed-mega'] = 0;
            percentages['bed-manor'] = 0;
            console.log(`🚫 Solos excluded: avg final kills ${avgStats.final_kills.toFixed(1)} > 7`);
        }

        if (avgStats.final_kills > 5) {
            percentages['bed-squads'] = 0
            console.log(`🚫 Beds excluded: avg final kills ${avgStats.final_kills.toFixed(1)} > 5`);
        }
        
        // Duos: Durchschnitt > 14 Final Kills = UNMÖGLICH
        if (avgStats.final_kills > 14) {
            percentages['bed-duos'] = 0;
            console.log(`🚫 Duos excluded: avg final kills ${avgStats.final_kills.toFixed(1)} > 14`);
        }
        
        // Normalisiere zurück auf 100% nach den Exclusions
        const total = Object.values(percentages).reduce((sum, val) => sum + Math.max(0, val), 0);
        if (total > 0) {
            for (const mode of Object.keys(percentages)) {
                percentages[mode] = Math.max(0, Math.round((percentages[mode] / total) * 100));
            }
        } else {
            // Fallback wenn alle ausgeschlossen wurden
            console.warn(`⚠️ All modes excluded for ${username}, using fallback`);
            percentages['bed-solos'] = 33;
            percentages['bed-duos'] = 33;
            percentages['bed-squads'] = 34;
        }
        
        // Debug-Info
        console.log(`📊 ${username} History (${history.length} games):`);
        console.log(`   Avg: Beds ${avgStats.beds_destroyed.toFixed(1)}, Kills ${avgStats.kills.toFixed(1)}, FK ${avgStats.final_kills.toFixed(1)}, Deaths ${avgStats.deaths.toFixed(1)}`);
        console.log(`   Result: ${this.formatPercentages(percentages)}`);
    }

    return percentages;
}

    formatPercentages(percentages) {
        const sorted = Object.entries(percentages)
            .sort(([, a], [, b]) => b - a)
            .filter(([, percentage]) => percentage > 0);
        
        return sorted.map(([mode, percentage]) => {
            const modeDisplay = mode.split('-').map(word => 
                word.charAt(0).toUpperCase() + word.slice(1)
            ).join(' ');
            return `${modeDisplay}: ${percentage}%`;
        }).join(' | ');
    }

    clearHistory(username) {
        this.gameHistory.delete(username.toLowerCase());
    }
}

// Start the bot
const bot = new HiveSnipingBot();

// Handle graceful shutdown
process.on('SIGINT', async () => {
    console.log('\n🛑 Shutting down bot...');
    
    // Clear all intervals
    for (const interval of bot.queuePredictionMessages.values()) {
        clearInterval(interval);
    }
    
    if (bot.lastSeenUpdateInterval) {
        clearInterval(bot.lastSeenUpdateInterval);
    }
    
    await bot.saveBotData();
    process.exit(0);
});

process.on('unhandledRejection', (reason, promise) => {
    console.error('❌ Unhandled Rejection at:', promise, 'reason:', reason);
});