<?php declare(strict_types = 1);

/**
 * This file is part of the Nextras\Orm library.
 * @license    MIT
 * @link       https://github.com/nextras/orm
 */

namespace Nextras\Orm\Collection\Helpers;

use Nextras\Orm\Entity\Reflection\PropertyMetadata;


class ValueReference
{
	/** @var bool */
	public $isFromHasManyResult;

	/** @var mixed */
	public $value;

	/** @var PropertyMetadata */
	public $propertyMetadata;


	public function __construct(bool $isFromHasManyResult, $value, PropertyMetadata $propertyMetadata)
	{
		$this->isFromHasManyResult = $isFromHasManyResult;
		$this->value = $value;
		$this->propertyMetadata = $propertyMetadata;
	}
}
