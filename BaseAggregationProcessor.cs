using bgTeam;
using bgTeam.DataAccess;
using Microsoft.Extensions.Logging;
using Prometheus;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Tolar.Reports.Common;
using Tolar.Reports.Common.Extenisons;
using Tolar.Reports.Common.Prometheus;
using Tolar.Reports.Common.Services.Impl.Common;
using Tolar.Reports.Domain;
using Tolar.Reports.Domain.Entities;
using Tolar.Reports.Domain.Entities.Aggregations;
using Tolar.Reports.Domain.Enumerations;
using Tolar.Reports.Domain.Extensions;

namespace Tolar.Reports.AggregationCreatorApp.Processors
{
    public abstract class BaseAggregationProcessor<TEntity, TAggr> : UniversalOutputService, IAggregationProcessor
        where TEntity : class, IEntity, new()
        where TAggr : AggregationBase, new()
    {
        protected readonly SemaphoreSlim _semaphore;

        private const int _delayInSec = 10;
        private readonly ICommonSettings _settings;

        private readonly string[] _labelsNames = new[] { "input" };
        private readonly string _inputName = $"{typeof(TEntity).Name}_{typeof(TAggr).Name}";
        private readonly Histogram _processDuration;

        private readonly Counter _processedRecordsCount;

        protected BaseAggregationProcessor(
            ILogger<BaseAggregationProcessor<TEntity, TAggr>> logger,
            IMapperBase mapper,
            IRepository repository,
            ICrudService crudService,
            IConnectionFactory connectionFactory,
            ICommonSettings settings)
           : base(logger, mapper, connectionFactory, crudService, repository, null)
        {
            _semaphore = new SemaphoreSlim(settings.MaxThreadsCount, settings.MaxThreadsCount);
            _settings = settings;

            _processDuration = Metrics.CreateHistogram(
            "process_of_one_batch_duration",
            "Time that takes to process one batch of raw data",
            PrometheusDefault.CreateConfig(_labelsNames));

            _processedRecordsCount = Metrics.CreateCounter(
            "process_records",
            "Number of processed records",
            PrometheusDefault.CreateCounterConfig(_labelsNames));
        }

        public async Task ProcessAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation($"Started {GetType().Name}");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    await ExecuteIterationAsync();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Failed to process upsert aggregation {typeof(TAggr).Name} by {typeof(TEntity).Name} data.");
                    await Task.Delay(TimeSpan.FromSeconds(_delayInSec), stoppingToken);
                }
            }
        }

        protected virtual async Task UpsertAggregationAsync(
            TAggr aggregationKey,
            TAggr aggregationFromDb,
            IEnumerable<TEntity> entities,
            IDbConnection connection,
            IDbTransaction transaction)
        {
            if (aggregationFromDb != null && aggregationFromDb.IsCompleted)
            {
                _logger.LogInformation($"Already exist legacy aggregation {typeof(TAggr).Name} hallid {aggregationFromDb.HallId}, projectid {aggregationFromDb.ProjectId}, currency {aggregationFromDb.CurrencyCode}, timestamp {aggregationFromDb.Timestamp}");
                return;
            }

            var aggregation = aggregationFromDb == null ? aggregationKey : aggregationFromDb;

            var oldAggrValues = _mapper.Map(aggregation, Activator.CreateInstance<TAggr>());

            await UpdateIndicatorsAsync(aggregation, entities, connection, transaction);

            if (aggregationFromDb == null)
            {
                await _crudService.InsertAsync(aggregation, connection, transaction).ConfigureAwait(false);
                _logger.LogDebug($"{typeof(TAggr).Name} ({aggregation.Timestamp}, {aggregation.TimestampUnix}, {aggregation.CurrencyCode}, {aggregation.HallId}) created on {typeof(TEntity).Name}s.");
            }
            else
            {
                await UpdateAggregationAsync(aggregation, connection, transaction, oldAggrValues);
                _logger.LogDebug($"{typeof(TAggr).Name} ({aggregation.Timestamp}, {aggregation.TimestampUnix}, {aggregation.CurrencyCode}, {aggregation.HallId}) updated on {typeof(TEntity).Name}s.");
            }
        }

        protected async Task ProcessGroupAsync(TAggr aggregationKey, IEnumerable<TEntity> entities)
        {
            using var connection = await _connectionFactory.CreateAsync().ConfigureAwait(false);
            using var transaction = connection.BeginTransaction();

            try
            {
                var aggregationFromDb = await GetDbObjectAsync(aggregationKey, connection, transaction).ConfigureAwait(false);
                await UpsertAggregationAsync(aggregationKey, aggregationFromDb, entities, connection, transaction);
                var aggregation = aggregationFromDb ?? aggregationKey;
                await UpdateProcessedEntitiesAsync(entities, connection, transaction);
                await SaveEventAggregationAsync(entities, aggregation, connection, transaction);

                transaction.Commit();

                _processedRecordsCount.WithLabels(_inputName).Inc(entities.Count());
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Failed to upsert aggregation {typeof(TAggr).Name} ({aggregationKey.Id}, {aggregationKey.Timestamp}, {aggregationKey.TimestampUnix}, {aggregationKey.CurrencyCode}, {aggregationKey.HallId}) with entities {typeof(TEntity).Name}s (count {entities.Count()})");

                transaction.Rollback();

                await Task.Delay(TimeSpan.FromSeconds(_delayInSec / 2));
            }

            _semaphore.Release();
        }

        protected abstract Task UpdateAggregationAsync(TAggr aggregation, IDbConnection connection, IDbTransaction transaction, TAggr oldValues = null);

        protected abstract Task MakeAggregationAsync(IEnumerable<TEntity> data);

        protected abstract Task UpdateIndicatorsAsync(TAggr aggregation, IEnumerable<TEntity> entities, IDbConnection connection, IDbTransaction transaction);

        protected abstract Task<IEnumerable<TEntity>> GetDataAsync(int recordsCount);

        protected abstract Task UpdateProcessedEntitiesAsync(IEnumerable<TEntity> entities, IDbConnection connection, IDbTransaction transaction);

        private async Task ExecuteIterationAsync()
        {
            var entities = await GetDataAsync(_settings.DataBatchLength);

            if (!entities.Any())
            {
                _logger.LogInformation($"No data in table {typeof(TEntity).Name}, delay {_delayInSec} sec");
                await Task.Delay(TimeSpan.FromSeconds(_delayInSec));
                return;
            }

            foreach (var entity in entities)
            {
                var timestamp = new DateTime(entity.Timestamp.Year, entity.Timestamp.Month, entity.Timestamp.Day, entity.Timestamp.Hour, 0, 0, DateTimeKind.Utc);

                entity.Timestamp = timestamp;
                entity.TimestampUnix = timestamp.ToUnixTime();
            }

            using (_processDuration.MeasureDuration(_inputName))
            {
                await MakeAggregationAsync(entities);
            }

            _logger.LogInformation($"Finished iteration {GetType().Name} with records count {entities.Count()}");
        }

        protected async Task SaveEventAggregationAsync(
            IEnumerable<TEntity> entities,
            TAggr aggregation,
            IDbConnection connection,
            IDbTransaction transaction)
        {
            var eventType = GetEventType();
            if (eventType == EventType.None)
            {
                return;
            }

            var arr = entities.ToArray();
            foreach (var entity in arr)
            {
                var eventAggr = new EventAggregation()
                {
                    EventId = entity.Id,
                    AggregationId = aggregation.Id,
                    EventType = eventType,
                };

                await _crudService.InsertAsync(eventAggr, connection: connection, transaction: transaction);
            }
        }

        private static EventType GetEventType()
        {
            var type = typeof(TEntity);
            return type.Name switch
            {
                nameof(Bet) => EventType.Bet,
                nameof(Win) => EventType.Win,
                nameof(Refund) => EventType.Refund,
                nameof(Rollback) => EventType.Rollback,
                nameof(SuccessDeposit) => EventType.Deposit,
                nameof(SuccessWithdrawal) => EventType.Withdrawal,
                nameof(BonusActivation) => EventType.BonusActivation,
                nameof(BonusCancellation) => EventType.BonusCancellation,
                nameof(BonusSubscription) => EventType.BonusSubscription,
                nameof(BonusUnSubscription) => EventType.BonusUnsubscription,
                nameof(BonusWagering) => EventType.BonusWagering,
                nameof(BonusStatusChanged) => EventType.BonusStatusChanged,
                nameof(GameStart) => EventType.GameStart,
                _ => EventType.None
            };
        }
    }
}
