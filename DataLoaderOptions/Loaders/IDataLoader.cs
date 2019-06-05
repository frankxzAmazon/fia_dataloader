namespace DataLoaderOptions
{
    interface IDataLoader
    {
        /// <summary>
        /// Loads the data to SQL.
        /// </summary>
        void LoadToSql();

        /// <summary>
        /// Gets or sets a value indicating whether we want [to load] the data.
        /// </summary>
        /// <value>
        ///   <c>true</c> if we want [to load]; otherwise, <c>false</c>.
        /// </value>
        bool ToLoad { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether we're in testing mode or not. If we're in testing mode,
        /// then we have to load the data to the development database.
        /// </summary>
        /// <value>
        ///   <c>true</c> if [testing mode]; otherwise, <c>false</c>.
        /// </value>
        bool useDevDB { get; set; }
    }
}